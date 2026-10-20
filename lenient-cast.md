# Lenient cast (`::~`) gotchas

- `::~` is one operator: `x:: ~long` is rejected, `x ::~ long` is fine.
- Parameters work as before: `?::long` and `??::long` are strict casts of a parameter, `?::~long`, `?p::~long` and `??::~long` are lenient ones. Mind the order: `?::~long` is lenient, `?::long` is strict.
- `x?::long` is not a lenient cast: it is a parameter after an expression, and fails.
- Only the cast written with `::~` is lenient: `x::~long::double` is lenient at `long`, `x::long::~double` is strict at `long`.
- Leniency only applies when the cast directly wraps the union field: `TRIM(x)::~keyword` still fails on the union.
- `TO_LONG(x)` and the other `TO_*` functions stay strict.
- On a single-typed field, `::~` behaves exactly like `::`, errors included.
- Leniency is per index type, not per value: `"abc"::~long` is still null with an evaluation warning.
- It still fails when no index type converts, and on unions with an unsupported type such as `rank_feature`.
- Not lenient yet when the union only exists across subquery branches (`FROM (FROM a), (FROM b) | EVAL y = f::~long`): it fails like `::`.
- Dev builds only, and it falls back to the strict error when a node is older than `esql_lenient_union_cast`.
- The `::~` token is lexed in every build, only the parser rule is dev-gated: a lexer predicate on a symbol token would slow down every query (see `EsqlBaseLexer.g4`).
