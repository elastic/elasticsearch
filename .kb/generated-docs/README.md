# ES|QL Generated Documentation

This directory contains auto-generated documentation for the ES|QL codebase. Documents are created and updated by Claude Code based on source analysis.

## Chapters

| Chapter | Title | Key Files Tracked |
|---------|-------|-------------------|
| [I](./chapter-01-request-response.md) | Request/Response Lifecycle | `action/Rest*.java`, `plugin/TransportEsqlQueryAction.java`, `session/`, `execution/` |
| [II](./chapter-02-monitoring.md) | Query Management & Monitoring | Async result management, query monitoring, `DriverStatus.java` |
| [III](./chapter-03-planner/00-overview.md) | The ES\|QL Planner | `parser/`, `analysis/`, `optimizer/`, `planner/mapper/`, `plan/`, `core/` |

### Chapter III Sub-documents

The planner chapter is split into focused documents:

| Document | Topic |
|----------|-------|
| [00-overview](./chapter-03-planner/00-overview.md) | Overview, AST mental model, plan types |
| [01-parser](./chapter-03-planner/01-parser.md) | Query string → Unresolved LogicalPlan |
| [02-pre-analyzer](./chapter-03-planner/02-pre-analyzer.md) | Discover index and enrich references |
| [03-analyzer](./chapter-03-planner/03-analyzer.md) | Resolution, type checking, verification |
| [04-logical-optimizer](./chapter-03-planner/04-logical-optimizer.md) | Rewrites, folding, pushdown |
| [05-mapper](./chapter-03-planner/05-mapper.md) | LogicalPlan → PhysicalPlan |
| [06-physical-optimizer](./chapter-03-planner/06-physical-optimizer.md) | Lucene pushdown, field extraction |
| [07-complete-example](./chapter-03-planner/07-complete-example.md) | Traced query through all stages |

## Regeneration

Each chapter includes metadata for incremental updates:

```
<!-- GENERATED DOC - DO NOT EDIT MANUALLY -->
<!-- Generated: YYYY-MM-DD -->
<!-- Source hash: <hash of key source files> -->
<!-- Generator: Claude Code -->
```

To regenerate a chapter, ask Claude Code to update it. The source hash helps determine if regeneration is needed after a pull.

### Checking for Updates

To see if source files have changed since generation:
```bash
# Get current hash of tracked files for a chapter
git ls-files -s <files> | git hash-object --stdin
```

Compare against the hash in the chapter header.

## Adding New Chapters

When creating a new chapter:
1. Name it `chapter-NN-descriptive-name.md`
2. Include the generation metadata header
3. List tracked source files in the header
4. Update this README's chapter table
