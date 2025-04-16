# mcp_mns_server.py

from mcp.server.fastmcp import FastMCP
from mns.account import Account
from mns.queue import QueueMeta, MNSExceptionBase, Message
from typing import List

# 创建 MCP 服务器
mcp = FastMCP("MNS MCP Server")

# 获取用户配置的 MNS 凭证和 endpoint
import os

ACCESS_KEY_ID = os.getenv("MNS_ACCESS_KEY_ID")
ACCESS_KEY_SECRET = os.getenv("MNS_ACCESS_KEY_SECRET")
ENDPOINT = os.getenv("MNS_ENDPOINT")

if not all([ACCESS_KEY_ID, ACCESS_KEY_SECRET, ENDPOINT]):
    raise ValueError(
        "Please set environment variables: MNS_ACCESS_KEY_ID, MNS_ACCESS_KEY_SECRET, MNS_ENDPOINT"
    )

# 初始化 MNS 账户
mns_account = Account(ENDPOINT, ACCESS_KEY_ID, ACCESS_KEY_SECRET)

@mcp.tool()
def create_mns_queue(queue_name: str) -> str:
    """
    创建一个 MNS 队列。

    Args:
        queue_name (str): 队列名称。

    Returns:
        str: 创建结果信息。
    """
    try:
        # 获取队列实例
        queue = mns_account.get_queue(queue_name)
        queue_meta = QueueMeta()

        # 尝试创建队列
        queue_url = queue.create(queue_meta)
        return f"Create Queue Succeed! QueueName: {queue_name}, QueueURL: {queue_url}"
    except MNSExceptionBase as e:
        if e.type == "QueueAlreadyExist":
            return f"Queue already exists: {queue_name}. Please delete it before creating or use it directly."
        return f"Create Queue Fail! Exception: {e}"

@mcp.tool()
async def delete_queue(queue_name: str) -> str:
    """
    删除 MNS 队列。

    Args:
        queue_name (str): 队列名称。

    Returns:
        str: 删除结果信息。
    """
    try:
        queue = mns_account.get_queue(queue_name)
        queue.delete()
        return f"Delete Queue Succeed! QueueName: {queue_name}"
    except MNSExceptionBase as e:
        return f"Delete Queue Fail! Exception: {e}"

@mcp.tool()
async def send_message(queue_name: str, message_body: str) -> str:
    """
    发送消息到指定队列。

    Args:
        queue_name (str): 队列名称。
        message_body (str): 消息内容。

    Returns:
        str: 发送结果信息。
    """
    try:
        queue = mns_account.get_queue(queue_name)
        msg = Message(message_body)
        response = queue.send_message(msg)
        return (
            f"Send Message Succeed! QueueName: {queue_name}, "
            f"MessageID: {response.message_id}, ReceiptHandle: {response.receipt_handle}"
        )
    except MNSExceptionBase as e:
        if e.type == "QueueNotExist":
            return f"Queue not exist: {queue_name}. Please create the queue first."
        return f"Send Message Fail! Exception: {e}"
    
@mcp.tool()
async def receive_and_delete_messages(queue_name: str, wait_seconds: int = 3) -> List[str]:
    """
    接收并删除队列中的消息。

    Args:
        queue_name (str): 队列名称。
        wait_seconds (int): 长轮询等待时间（秒）。

    Returns:
        List[str]: 收到的消息列表。
    """
    try:
        queue = mns_account.get_queue(queue_name)
        results = []

        while True:
            # 接收消息
            recv_msg = queue.receive_message_with_str_body(wait_seconds)
            if not recv_msg:
                break

            results.append(
                f"Received Message! ID: {recv_msg.message_id}, Body: {recv_msg.message_body}"
            )

            # 删除消息
            try:
                queue.delete_message(recv_msg.receipt_handle)
                results.append(f"Deleted Message! ID: {recv_msg.message_id}")
            except MNSExceptionBase as delete_e:
                results.append(f"Failed to Delete Message! ID: {recv_msg.message_id}, Exception: {delete_e}")

        if not results:
            return ["Queue is empty!"]

        return results
    except MNSExceptionBase as e:
        if e.type == "QueueNotExist":
            return [f"Queue not exist: {queue_name}. Please create the queue first."]
        return [f"Receive Message Fail! Exception: {e}"]

@mcp.tool()
def list_queues(prefix: str = "") -> List[str]:
    """
    获取给定前缀的所有队列。如果未提供前缀，则列出所有队列。

    Args:
        prefix (str): 队列名称的前缀，默认为空字符串。

    Returns:
        List[str]: 包含队列信息的列表，每条信息格式为 "QueueName: <name>, QueueURL: <url>"。
    """
    try:
        results = []
        marker = ""  # 初始化分页标记

        while True:
            # 调用 Account 的 list_queue 方法获取队列列表
            queue_urls, next_marker = mns_account.list_queue(prefix=prefix, ret_number=100, marker=marker)

            if not queue_urls:
                break  # 没有更多队列时退出循环

            # 格式化返回结果
            for queue_url in queue_urls:
                queue_name = queue_url.split("/")[-1]  # 从 URL 中提取队列名称
                results.append(f"QueueName: {queue_name}, QueueURL: {queue_url}")

            if not next_marker:
                break  # 如果没有更多队列，退出循环
            marker = next_marker  # 更新分页标记

        if not results:
            return [f"No queues found with prefix: {prefix}" if prefix else "No queues found."]

        return results
    except MNSExceptionBase as e:
        return [f"List Queues Fail! Exception: {e}"]

def main():
    # 启动 MCP 服务器
    mcp.run(transport='stdio')

if __name__ == "__main__":
    print("Starting MCP server...")
    main()