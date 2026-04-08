<!-- GENERATED DOC - DO NOT EDIT MANUALLY -->
<!-- Generated: 2026-02-05 -->
<!-- Generator: Claude Code -->

# Stage 1: Parser

**Input**: Query string
**Output**: Unresolved `LogicalPlan`

The parser transforms the raw query text into a tree of plan and expression nodes. At this stage, nothing is resolved—field names are just strings, functions are just names, and we don't know if anything actually exists.

---

## Architecture

```
Query String: "FROM logs | WHERE status >= 400 | LIMIT 100"
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │    EsqlBaseLexer    │  ANTLR-generated
                         │    (tokenization)   │  from EsqlBaseLexer.g4
                         └──────────┬──────────┘
                                    │ Token stream
                                    ▼
                         ┌─────────────────────┐
                         │   EsqlBaseParser    │  ANTLR-generated
                         │   (parse tree/CST)  │  from EsqlBaseParser.g4
                         └──────────┬──────────┘
                                    │ Parse tree
                                    ▼
                         ┌─────────────────────┐
                         │     AstBuilder      │  Visitor pattern
                         │                     │
                         │  LogicalPlanBuilder │  Creates plan nodes
                         │  ExpressionBuilder  │  Creates expressions
                         └──────────┬──────────┘
                                    │
                                    ▼
                           LogicalPlan (unresolved)
```

---

## Key Components

### EsqlParser (Entry Point)

**File**: `parser/EsqlParser.java`

```java
public class EsqlParser {
    public static final int MAX_LENGTH = 1_000_000;  // 1MB query size limit

    public LogicalPlan parseQuery(String query, QueryParams params) {
        if (query.length() > MAX_LENGTH) {
            throw new ParsingException("Query too large");
        }

        // 1. Tokenize
        EsqlBaseLexer lexer = new EsqlBaseLexer(CharStreams.fromString(query));

        // 2. Handle query parameters (?)
        TokenSource tokenSource = new ParametrizedTokenSource(lexer, params);
        CommonTokenStream tokenStream = new CommonTokenStream(tokenSource);

        // 3. Parse to tree
        EsqlBaseParser parser = new EsqlBaseParser(tokenStream);
        ParserRuleContext tree = parser.singleStatement();

        // 4. Build AST
        return new AstBuilder(params).plan(tree);
    }
}
```

### LogicalPlanBuilder (Plan Nodes)

**File**: `parser/LogicalPlanBuilder.java`

Creates plan nodes by visiting the parse tree. Each ES|QL command becomes a plan node:

```java
public class LogicalPlanBuilder extends ExpressionBuilder {

    @Override
    public LogicalPlan visitFromCommand(FromCommandContext ctx) {
        // FROM logs → UnresolvedRelation
        String indexPattern = visitIndexPattern(ctx.indexPattern());
        return new UnresolvedRelation(source(ctx), indexPattern, ...);
    }

    @Override
    public LogicalPlan visitWhereCommand(WhereCommandContext ctx) {
        // WHERE condition → Filter wrapping child
        Expression condition = expression(ctx.booleanExpression());
        return new Filter(source(ctx), input, condition);
    }

    @Override
    public LogicalPlan visitLimitCommand(LimitCommandContext ctx) {
        // LIMIT n → Limit wrapping child
        Literal limit = (Literal) visit(ctx.INTEGER_LITERAL());
        return new Limit(source(ctx), input, limit);
    }

    @Override
    public LogicalPlan visitEvalCommand(EvalCommandContext ctx) {
        // EVAL x = expr → Eval with named expressions
        List<Alias> fields = visitFields(ctx.fields());
        return new Eval(source(ctx), input, fields);
    }

    @Override
    public LogicalPlan visitStatsCommand(StatsCommandContext ctx) {
        // STATS agg BY grouping → Aggregate
        List<Expression> aggregates = visitAggregates(ctx);
        List<Expression> groupings = visitGroupings(ctx);
        return new Aggregate(source(ctx), input, Aggregate.AggregateType.STANDARD, groupings, aggregates);
    }

    @Override
    public LogicalPlan visitSortCommand(SortCommandContext ctx) {
        // SORT field DESC → OrderBy
        List<Order> orders = visitOrders(ctx.orderExpression());
        return new OrderBy(source(ctx), input, orders);
    }
}
```

### ExpressionBuilder (Expression Nodes)

**File**: `parser/ExpressionBuilder.java`

Creates expression nodes for conditions, calculations, and references:

```java
public class ExpressionBuilder extends EsqlBaseParserBaseVisitor<Object> {

    @Override
    public Expression visitQualifiedName(QualifiedNameContext ctx) {
        // field_name → UnresolvedAttribute
        String name = ctx.getText();
        return new UnresolvedAttribute(source(ctx), name);
    }

    @Override
    public Expression visitFunctionExpression(FunctionExpressionContext ctx) {
        // FUNC(args) → UnresolvedFunction
        String name = ctx.functionName().getText();
        List<Expression> args = expressions(ctx.booleanExpression());
        return new UnresolvedFunction(source(ctx), name, FunctionResolutionStrategy.DEFAULT, args);
    }

    @Override
    public Expression visitComparison(ComparisonContext ctx) {
        // a >= b → GreaterThanOrEqual (or other comparison)
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);
        return switch (ctx.comparisonOperator().getText()) {
            case ">=" -> new GreaterThanOrEqual(source(ctx), left, right);
            case ">"  -> new GreaterThan(source(ctx), left, right);
            case "<=" -> new LessThanOrEqual(source(ctx), left, right);
            case "<"  -> new LessThan(source(ctx), left, right);
            case "==" -> new Equals(source(ctx), left, right);
            case "!=" -> new NotEquals(source(ctx), left, right);
            // ...
        };
    }

    @Override
    public Expression visitArithmeticBinary(ArithmeticBinaryContext ctx) {
        // a + b → Add (or other arithmetic)
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);
        return switch (ctx.operator.getType()) {
            case PLUS  -> new Add(source(ctx), left, right);
            case MINUS -> new Sub(source(ctx), left, right);
            case ASTERISK -> new Mul(source(ctx), left, right);
            case SLASH -> new Div(source(ctx), left, right);
            case PERCENT -> new Mod(source(ctx), left, right);
            // ...
        };
    }

    @Override
    public Literal visitIntegerLiteral(IntegerLiteralContext ctx) {
        // 42 → Literal(42, INTEGER)
        String text = ctx.getText();
        Number value = StringUtils.parseIntegral(text);
        DataType type = DataType.fromJava(value);
        return new Literal(source(ctx), value, type);
    }

    @Override
    public Literal visitStringLiteral(StringLiteralContext ctx) {
        // "hello" → Literal("hello", KEYWORD)
        String value = unquote(ctx.getText());
        return new Literal(source(ctx), value, DataType.KEYWORD);
    }
}
```

---

## What Gets Created (Unresolved)

At the end of parsing, we have a tree of plan nodes containing expression trees. Everything is **unresolved**:

| Parsed Element | Becomes | Status |
|----------------|---------|--------|
| `FROM logs` | `UnresolvedRelation("logs")` | Don't know if index exists |
| `status` | `UnresolvedAttribute("status")` | Don't know if field exists |
| `ABS(x)` | `UnresolvedFunction("ABS", [x])` | Don't know if function exists |
| `400` | `Literal(400, INTEGER)` | Resolved (literals are always resolved) |
| `>=` | `GreaterThanOrEqual(left, right)` | Unresolved until children resolve |

---

## Example: Parsing a Query

Query: `FROM logs | WHERE status >= 400 | LIMIT 100`

### Token Stream

```
FROM, IDENTIFIER("logs"), PIPE, WHERE, IDENTIFIER("status"), GTE, INTEGER(400), PIPE, LIMIT, INTEGER(100)
```

### Parse Tree (CST)

```
singleStatement
└── query
    └── sourceCommand
        └── fromCommand
            └── indexPattern: "logs"
    └── processingCommand
        └── whereCommand
            └── booleanExpression
                └── comparison
                    └── left: qualifiedName("status")
                    └── operator: ">="
                    └── right: integerLiteral(400)
    └── processingCommand
        └── limitCommand
            └── INTEGER_LITERAL: 100
```

### Resulting LogicalPlan (AST)

```
Limit[100]
└── Filter[GreaterThanOrEqual[UnresolvedAttribute("status"), Literal(400)]]
    └── UnresolvedRelation["logs"]
```

In tree notation:
```
Limit
├── limit: Literal(100, INTEGER)
└── child: Filter
    ├── condition: GreaterThanOrEqual
    │   ├── left: UnresolvedAttribute("status")
    │   └── right: Literal(400, INTEGER)
    └── child: UnresolvedRelation("logs")
```

---

## Query Parameters

The parser handles parameterized queries where `?` placeholders are replaced with values:

```java
// Query: "FROM logs | WHERE status >= ?"
// Params: [400]

// ParametrizedTokenSource replaces ? tokens with the literal values
// Result: UnresolvedAttribute replaced with Literal(400)
```

This happens at the token level, before the AST is built.

---

## Error Handling

Syntax errors are caught during parsing:

```java
// Invalid: "FROM | WHERE"
// Error: "mismatched input '|' expecting {<EOF>, PIPE, ...}"

// Invalid: "FROM logs WHERE"  (missing pipe)
// Error: "missing '|' at 'WHERE'"
```

Parse errors include line and column information from the `Source` object.

---

## Files

| File | Purpose |
|------|---------|
| `parser/EsqlParser.java` | Entry point, orchestrates parsing |
| `parser/LogicalPlanBuilder.java` | Builds plan nodes from parse tree |
| `parser/ExpressionBuilder.java` | Builds expression nodes |
| `parser/AstBuilder.java` | Coordinates plan and expression building |
| `parser/ParametrizedTokenSource.java` | Handles query parameters |

---

## Next: [Pre-Analyzer](./02-pre-analyzer.md)

The pre-analyzer scans the unresolved plan to discover what external resources need to be resolved before analysis can proceed.
