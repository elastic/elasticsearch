# Atomic index rename: proof of concept, tradeoffs and open decisions

Status: proof of concept. This document records what the prototype does, every
shortcut taken to get it working end to end, and the decisions that need to be
made before any of it is production quality. Tracking issue:
elastic/elasticsearch-team#1743 (public ask: #37880).

## What the prototype does

A new master action, `indices:admin/rename` (`TransportRenameIndexAction`),
renames an index in a single cluster state update by rewriting the entry in
`ProjectMetadata`, `RoutingTable` and `ClusterBlocks` under the new name while
keeping the index UUID. Data nodes recognise the rename and update their
in-memory `IndexService` and `IndexShard` objects in place; no shard is closed,
recovered or relocated.

The index UUID is the identity of an index throughout. Everything keyed on the
UUID (node data paths, persisted cluster state, the stateless object store,
`IndicesService#indices`) survives a rename untouched. Everything keyed on the
name (`ProjectMetadata#indices`, `RoutingTable`, `ClusterBlocks`, snapshot
`IndexId`) has to be rewritten or tolerated.

## Tradeoffs taken to get the prototype working

Each item names the shortcut, why it was taken, and what the production answer
would need to be.

(filled in as the prototype evolves; see the sections below)

## Decisions that need to be made

(filled in as the prototype evolves; see the sections below)
