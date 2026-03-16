
from __future__ import annotations

import streamlit as st
from typing import List, Dict, Any

import agent_backend as backend

st.set_page_config(page_title="FinTech Agent Chat", page_icon="📈", layout="wide")

MAX_EXCHANGES = 3
AGENT_OPTIONS = ["Single Agent", "Multi-Agent"]
MODEL_OPTIONS = ["gpt-4o-mini", "gpt-4o"]


def init_state():
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "exchange_count" not in st.session_state:
        st.session_state.exchange_count = 0


def clear_conversation():
    st.session_state.messages = []
    st.session_state.exchange_count = 0


def format_history_for_agent(messages: List[Dict[str, Any]], max_exchanges: int = MAX_EXCHANGES) -> str:
    """
    Build a compact history string from up to the last 3 user/assistant exchanges.
    This is passed into the backend as part of the current question so the agent can
    resolve references like 'that', 'the two', or 'it'.
    """
    recent = messages[-max_exchanges * 2:] if messages else []
    lines = []
    for msg in recent:
        role = msg.get("role", "assistant")
        content = msg.get("content", "")
        role_name = "User" if role == "user" else "Assistant"
        lines.append(f"{role_name}: {content}")
    return "\n".join(lines)


def build_augmented_question(current_question: str) -> str:
    history = format_history_for_agent(st.session_state.messages, MAX_EXCHANGES)
    if not history:
        return current_question

    return f"""
You are answering in a multi-turn chat.
Use the recent conversation history to resolve follow-up references such as
"that", "the two", "it", "they", or omitted company names.
Base the answer on the latest user request while using history only for reference resolution.

Conversation history:
{history}

Current user question:
{current_question}
""".strip()


def render_message(msg: Dict[str, Any]):
    role = msg["role"]
    with st.chat_message("user" if role == "user" else "assistant"):
        st.markdown(msg["content"])
        if role == "assistant":
            meta = []
            if msg.get("architecture"):
                meta.append(f"Architecture: **{msg['architecture']}**")
            if msg.get("model"):
                meta.append(f"Model: **{msg['model']}**")
            if msg.get("tools"):
                meta.append(f"Tools: `{', '.join(msg['tools'])}`")
            if msg.get("agents"):
                meta.append(f"Agents: `{', '.join(msg['agents'])}`")
            if msg.get("elapsed_sec") is not None:
                meta.append(f"Time: **{msg['elapsed_sec']:.2f}s**")
            if meta:
                st.caption(" | ".join(meta))


def call_backend(architecture: str, model_name: str, user_question: str) -> Dict[str, Any]:
    backend.set_active_model(model_name)
    enriched_question = build_augmented_question(user_question)

    if architecture == "Single Agent":
        result = backend.run_single_agent(enriched_question, verbose=False)
        return {
            "answer": result.answer,
            "architecture": architecture,
            "model": model_name,
            "tools": result.tools_called,
            "agents": [result.agent_name],
            "elapsed_sec": None,
            "raw": result,
        }

    result = backend.run_multi_agent(
        enriched_question,
        verbose=False,
        routing_text=user_question,   # 只用当前这一轮做 routing
    )
    return {
        "answer": result["final_answer"],
        "architecture": architecture,
        "model": model_name,
        "tools": [t for r in result["agent_results"] for t in r.tools_called],
        "agents": [r.agent_name for r in result["agent_results"]],
        "elapsed_sec": result.get("elapsed_sec"),
        "raw": result,
    }


def main():
    init_state()

    st.title("📈 FinTech Agent Chat")
    st.write("Chat with your Single Agent or Multi-Agent finance assistant.")

    with st.sidebar:
        st.header("Controls")
        architecture = st.selectbox("Agent selector", AGENT_OPTIONS, index=1)
        model_name = st.selectbox("Model selector", MODEL_OPTIONS, index=0)
        if st.button("Clear conversation", use_container_width=True):
            clear_conversation()
            st.rerun()
        st.divider()
        st.caption("The app keeps up to the last 3 exchanges as conversational memory.")

    for msg in st.session_state.messages:
        render_message(msg)

    user_input = st.chat_input("Ask a finance question...")
    if user_input:
        st.session_state.messages.append({"role": "user", "content": user_input})
        render_message(st.session_state.messages[-1])

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                payload = call_backend(architecture, model_name, user_input)

            st.markdown(payload["answer"])
            meta = []
            meta.append(f"Architecture: **{payload['architecture']}**")
            meta.append(f"Model: **{payload['model']}**")
            if payload.get("tools"):
                meta.append(f"Tools: `{', '.join(payload['tools'])}`")
            if payload.get("agents"):
                meta.append(f"Agents: `{', '.join(payload['agents'])}`")
            if payload.get("elapsed_sec") is not None:
                meta.append(f"Time: **{payload['elapsed_sec']:.2f}s**")
            st.caption(" | ".join(meta))

        st.session_state.messages.append({
            "role": "assistant",
            "content": payload["answer"],
            "architecture": payload["architecture"],
            "model": payload["model"],
            "tools": payload.get("tools", []),
            "agents": payload.get("agents", []),
            "elapsed_sec": payload.get("elapsed_sec"),
        })


if __name__ == "__main__":
    main()
