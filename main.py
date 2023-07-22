from web3 import Web3
from web3.types import LogReceipt, HexBytes
from web3._utils.filters import LogFilter
import threading
import queue
from datetime import datetime
import asyncio
from info import infura_goerli_url, goerli_ms_sc_adr, goerli_ms_sc_abi, \
    sibr_net_url, sibr_ms_sc_adr, sibr_ms_sc_abi, abi_of_sc, provider_of_sc

guradian_adr = '0xe52FB548417eE451192200fdAf8Fa1511daB2300'
guradian_adr_pk = 'c0fcfbef21969b71702481d5023e5ef1cf07ad4a3f0da7d2d71cfdea0e0c7abc'

message_queue = queue.Queue()

eth_web3 = Web3(Web3.HTTPProvider(infura_goerli_url, request_kwargs={'timeout': 60}))
eth_contract = eth_web3.eth.contract(address=goerli_ms_sc_adr, abi=goerli_ms_sc_abi)
eth_event_filters = (eth_contract.events.IssueInited.create_filter(fromBlock='latest'))

sibr_web3 = Web3(Web3.HTTPProvider(sibr_net_url, request_kwargs={'timeout': 60}))
sibr_contract = sibr_web3.eth.contract(address=sibr_ms_sc_adr, abi=sibr_ms_sc_abi)
sibr_event_filters = (sibr_contract.events.IssueInited.create_filter(fromBlock='latest'))


def standard_trx_build_for_sc_call_with_gas(base_adr, provider_url: str) -> dict:
    if base_adr is None:
        return None
    web3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={'timeout': 60}))

    build_trx_config = {
        'chainId': web3.eth.chain_id,
        'from': base_adr,
        'nonce': web3.eth.get_transaction_count(base_adr),
        # 'gasPrice': web3.eth.gas_price,
        'maxFeePerGas': 30000000000,
        'maxPriorityFeePerGas': 3000000000,
    }
    gas_eddition = 1000
    if provider_url.find("infura") != -1:
        gas_eddition = 10000
    gas = web3.eth.estimate_gas(build_trx_config) + gas_eddition
    build_trx_config['gas'] = gas + int(gas * 0.2)

    return build_trx_config


def signIssueInMSW(guardian_adr, guardian_adr_pk, issue_id: int, sc_address, sc_abi, provider_url):
    web3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={'timeout': 60}))
    contract = web3.eth.contract(address=web3.to_checksum_address(sc_address), abi=sc_abi)
    build_trx_config = standard_trx_build_for_sc_call_with_gas(guardian_adr, provider_url)
    build_trx_config['gas'] += int(build_trx_config['gas'] * 0.2)

    f = contract.functions.signIssue(issue_id)
    unsigned_tx = f.build_transaction(build_trx_config)
    signed_tx = web3.eth.account.sign_transaction(unsigned_tx, guardian_adr_pk)
    tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
    # Wait for the transaction to be mined, and get the transaction receipt
    tx_receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
    print(tx_receipt)


def handle_event(event):
    print(f"handled event={event}")
    event_log_receipt: LogReceipt = event
    event_type = event_log_receipt.get('event', '')
    address_of_sc = event_log_receipt.get('address', None)
    if event_type == 'IssueInited':
        to_address = event_log_receipt.get('args').get('to')
        value_wei = event_log_receipt.get('args').get('value')
        issue_index = event_log_receipt.get('args').get('issueIndex')
        #trx_hash = event_log_receipt.get('transactionHash', HexBytes('0x0000')).hex()
        message_queue.put(
            {'type': 'handle_issue_inited',
             'sc_address': address_of_sc,
             #'tx_init_hash': trx_hash,
             'to_address': to_address,
             'value': value_wei,
             'issue_index': issue_index})


def read_queue_messages(message_queue):
    while True:
        # Retrieve and log message from the queue
        message = message_queue.get()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Received message: {message} (Timestamp: {timestamp})")
        m_type = message.get('type', '')
        if m_type == 'handle_issue_inited':
            m_id_in_contract = int(message.get('issue_index'))
            m_sc_adr = message.get('sc_address')
            signIssueInMSW(
                guardian_adr=guradian_adr,
                guardian_adr_pk=guradian_adr_pk,
                issue_id=m_id_in_contract,
                sc_address=m_sc_adr,
                sc_abi=abi_of_sc[m_sc_adr],
                provider_url=provider_of_sc[m_sc_adr])


async def eth_events_handler(event_filters, poll_interval):
    while True:
        if type(event_filters) is LogFilter:
            for SomeEventArrived in event_filters.get_new_entries():
                handle_event(SomeEventArrived)
            await asyncio.sleep(poll_interval)
        else:
            for event_filter in event_filters:
                for SomeEventArrived in event_filter.get_new_entries():
                    handle_event(SomeEventArrived)
                await asyncio.sleep(poll_interval)


async def sibr_events_handler(event_filters, poll_interval):
    while True:
        if type(event_filters) is LogFilter:
            for SomeEventArrived in event_filters.get_new_entries():
                handle_event(SomeEventArrived)
            await asyncio.sleep(poll_interval)
        else:
            for event_filter in event_filters:
                for SomeEventArrived in event_filter.get_new_entries():
                    handle_event(SomeEventArrived)
                await asyncio.sleep(poll_interval)



def listen_eth_events():
    web3 = Web3(Web3.HTTPProvider(infura_goerli_url, request_kwargs={'timeout': 60}))
    contract = web3.eth.contract(address=goerli_ms_sc_adr, abi=goerli_ms_sc_abi)

    event_filters = (contract.events.IssueInited.create_filter(fromBlock='latest'))

    # Start a separate thread to read messages from the queue
    queue_thread = threading.Thread(target=read_queue_messages, args=(message_queue,))
    queue_thread.start()

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(
            asyncio.gather(
                eth_events_handler(event_filters, 1)))
    finally:
        # close loop to free up system resources
        loop.close()


def listen_eth_and_sibr_events():
    #asyncio.create_task(eth_events_handler(eth_event_filters, 1))
    #asyncio.create_task(sibr_events_handler(sibr_event_filters, 1))

    # Start a separate thread to read messages from the queue
    queue_thread = threading.Thread(target=read_queue_messages, args=(message_queue,))
    queue_thread.start()

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(
            asyncio.gather(
                eth_events_handler(eth_event_filters, 1),
                sibr_events_handler(sibr_event_filters, 1)
            ))
    finally:
        # close loop to free up system resources
        loop.close()


def start_guardian_checker():
    #listen_eth_events()
    listen_eth_and_sibr_events()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    start_guardian_checker()
