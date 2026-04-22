# Coinbase Level 2 Replay Lab

A small Python project for replaying raw Coinbase Level 2 market data, rebuilding the visible order book, and exporting top-of-book features to CSV.

## Overview

This project takes raw JSONL market-data files collected from the Coinbase WebSocket feed and reconstructs the Level 2 order book through time.

The current goal is not strategy execution yet. The goal is to build a correct, replayable market-state dataset that can later be used for:

- microstructure analysis
- feature engineering
- execution simulation
- baseline trading research

## What the script does

The replay script:

1. reads a raw JSONL file line by line
2. parses Coinbase messages
3. rebuilds the visible Level 2 order book
4. processes `snapshot` and `l2update` messages
5. extracts top-of-book metrics after each actionable event
6. writes the result to a processed CSV file

## Reconstruction rules

The current replay logic follows these rules:

- `snapshot` replaces the full visible book
- `l2update` changes one or more price levels
- update size is treated as the new absolute size at that level
- if update size is `0`, that price level is removed
- the replay does **not** simulate matching or trade execution

## Input format

The script expects a raw JSONL file where each line has this structure:

```json
{
  "timestamp": "2026-04-21T23:17:56.123456+00:00",
  "message": {
    "type": "l2update",
    "...": "raw Coinbase payload"
  }
}