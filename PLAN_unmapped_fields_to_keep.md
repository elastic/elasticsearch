# Plan: Annotate `EsRelation` with an `UnmappedFieldsAttribute`

## Context / Goal

Add an analyzer rule (`DetermineUnmappedFieldsToKeep`) that annotates each non-LOOKUP
`EsRelation` with a new attribute (`UnmappedFieldsAttribute`) carrying an
`UnmappedFieldsPattern` that describes which additional unmapped source fields would
survive to the query output under `LOAD_ALL` mode.

Semantics of the pattern:
- Unmapped fields not mentioned in the query can only be kept or dropped — never renamed
  or transformed.
- Fields already in the `EsRelation` are excluded (mapped, partially mapped, or
  unmapped-but-explicitly-referenced).
- The result is a glob pattern describing which extra field names the query would
  propagate to the output.

Examples:
- `FROM idx | KEEP foo*` → fields matching `foo*`
- `FROM idx | DROP foo*` → everything except `foo*`
- `FROM idx | EVAL x = ...` → `x` is shadowed (dropped from unmapped candidates)
- `FROM idx | RENAME y AS x` → `x` is shadowed (dropped)

---

## Design changes since v1

The original v1 plan placed `unmappedFieldsToKeep()` on `Keep`, `Drop`, and `Rename`.
After review:

- **`Keep`, `Drop`, `Rename` do NOT need the method** — these nodes only exist pre-analysis.
- **`ResolvingProject` is the right carrier**: persists into the Finish Analysis batch and
  carries the KEEP/DROP/RENAME semantics via its `unmappedFieldsToKeep()` override.
- **`EsRelation` is annotated with `UnmappedFieldsAttribute`** added to its `attrs` list —
  **not** a separate raw field.

---

## Architecture

### Nodes in play during the Finish Analysis batch

| Node | Role |
|---|---|
| `ResolvingProject` | Carries `UnmappedFieldsPattern` from KEEP/DROP/RENAME; has `unmappedFieldsToKeep()` |
| `EsRelation` | Leaf; `DetermineUnmappedFieldsToKeep` appends `UnmappedFieldsAttribute` to its `attrs` |
| `DetermineUnmappedFieldsToKeep` | Runs **before** `ResolvedProjects`; computes pattern; annotates EsRelations |
| `ResolvedProjects` | Runs after; converts `ResolvingProject` → `Project` |

---

### `UnmappedFieldsPattern` record (unchanged)

```java
public record UnmappedFieldsPattern(List<String> includes, List<String> excludes) {
    public static final UnmappedFieldsPattern ALL  = new UnmappedFieldsPattern(List.of("*"), List.of());
    public static final UnmappedFieldsPattern NONE = new UnmappedFieldsPattern(List.of(),    List.of());
    public UnmappedFieldsPattern { includes = List.copyOf(includes); excludes = List.copyOf(excludes); }
}
```

---

### `MetadataAttribute`: make it `sealed`

Remove the `final` modifier; add `sealed ... permits UnmappedFieldsAttribute`.

```java
// MetadataAttribute.java
public sealed class MetadataAttribute extends FieldAttribute
    permits org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute {
    ...
}
```

Both classes are in the same unnamed module (no `module-info.java` for the main esql
plugin), so cross-package `permits` is valid.

---

### `UnmappedFieldsAttribute`

```
package org.elasticsearch.xpack.esql.plan.logical;
file:    x-pack/plugin/esql/src/main/java/.../plan/logical/UnmappedFieldsAttribute.java
```

```java
public final class UnmappedFieldsAttribute extends MetadataAttribute {
    public static final String ATTRIBUTE_NAME = "_unmapped_fields";

    private final UnmappedFieldsPattern pattern;

    public UnmappedFieldsAttribute(Source source, UnmappedFieldsPattern pattern) {
        super(source, ATTRIBUTE_NAME, DataType.NULL, false);
        this.pattern = pattern;
    }

    public UnmappedFieldsPattern pattern() { return pattern; }

    // Planning-time annotation only — never serialized.
    @Override public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("planning-time only");
    }
}
```

> The DataType doesn't matter much; `NULL` or a sentinel is fine since this attribute
> will never be fetched from a source.

---

### `EsRelation` changes

- **Remove** the `unmappedFieldsPattern` raw field and all associated methods
  (`unmappedFieldsPattern()`, `withUnmappedFieldsPattern()`).
- **Keep** `unmappedFieldsToKeep()` — returns `ALL` (used during recursive computation).
- **Keep** `equals()` / `hashCode()` unchanged — `attrs` is already compared, so adding a
  new attribute to the list automatically affects equality correctly.
- The attribute is appended by `DetermineUnmappedFieldsToKeep` via the standard
  `EsRelation` constructor: create a new `EsRelation` with `attrs` list extended by one
  `UnmappedFieldsAttribute`.

---

### `ResolvingProject`: always pass through `UnmappedFieldsAttribute`

When `DetermineUnmappedFieldsToKeep` creates a new `EsRelation` (with the attribute in
its output) and the tree is rebuilt, `ResolvingProject.replaceChild(newEsr)` is called.
The public constructor recomputes projections via `resolver.apply(newEsr.output())`.
The resolver (e.g., for `KEEP salary`) won't select `_unmapped_fields`, so the attribute
would be dropped.

Fix: in the constructor, after applying the resolver, append any `UnmappedFieldsAttribute`
instances from the child output that weren't already included:

```java
public ResolvingProject(Source source, LogicalPlan child,
        Function<List<Attribute>, List<? extends NamedExpression>> resolver,
        UnmappedFieldsPattern unmappedFieldsPattern) {
    this(source, child,
         projectionsWithUnmapped(resolver.apply(child.output()), child.output()),
         resolver, unmappedFieldsPattern);
}

private static List<? extends NamedExpression> projectionsWithUnmapped(
        List<? extends NamedExpression> base, List<Attribute> childOutput) {
    List<Attribute> unmappedAttrs = childOutput.stream()
        .filter(a -> a instanceof UnmappedFieldsAttribute)
        .toList();
    if (unmappedAttrs.isEmpty()) return base;
    Set<NameId> inBase = base.stream().map(NamedExpression::id).collect(toSet());
    List<NamedExpression> result = new ArrayList<>(base);
    for (Attribute a : unmappedAttrs) {
        if (inBase.contains(a.id()) == false) result.add(a);
    }
    return result;
}
```

We assume `_unmapped_fields` cannot be shadowed by RENAME or EVAL (true in the vast
majority of queries; can be revisited later).

---

### `DetermineUnmappedFieldsToKeep` rule

```java
private static class DetermineUnmappedFieldsToKeep extends Rule<LogicalPlan, LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        UnmappedFieldsPattern pattern;
        try {
            pattern = plan.unmappedFieldsToKeep();
        } catch (UnsupportedOperationException e) {
            pattern = UnmappedFieldsPattern.ALL; // fallback for binary/fork nodes
        }
        return annotate(plan, new UnmappedFieldsAttribute(Source.EMPTY, pattern));
    }

    // Post-order traversal using reference-identity (not equals) to detect changes.
    // EsRelation.equals() does not change when a new attribute is appended because
    // attrs is compared — but the OLD EsRelation doesn't have the attribute, so the
    // NEW one will be unequal. Standard transformUp therefore also works, but the
    // custom walk is explicit and safe.
    private static LogicalPlan annotate(LogicalPlan plan, UnmappedFieldsAttribute attr) {
        if (plan instanceof EsRelation esr) {
            return esr.indexMode() == IndexMode.LOOKUP ? esr
                 : new EsRelation(esr.source(), esr.indexPattern(), esr.indexMode(),
                       esr.originalIndices(), esr.concreteIndices(), esr.indexNameWithModes(),
                       append(esr.attrs(), attr));
        }
        List<LogicalPlan> children = plan.children();
        if (children.isEmpty()) return plan;
        boolean changed = false;
        List<LogicalPlan> newChildren = new ArrayList<>(children.size());
        for (LogicalPlan child : children) {
            LogicalPlan newChild = annotate(child, attr);
            newChildren.add(newChild);
            changed |= (newChild != child);
        }
        return changed ? plan.replaceChildren(newChildren) : plan;
    }

    private static List<Attribute> append(List<Attribute> attrs, Attribute extra) {
        List<Attribute> result = new ArrayList<>(attrs);
        result.add(extra);
        return result;
    }
}
```

---

### `unmappedFieldsToKeep()` chain (unchanged from end of last session)

| Class | Implementation |
|---|---|
| `LogicalPlan` | Stub — throws `UnsupportedOperationException` |
| `UnaryPlan` | Delegates to child |
| `EsRelation` | Returns `ALL` |
| `ResolvingProject` | Computes pattern from own `unmappedFieldsPattern` + child |
| `Eval` | Adds computed field names to child's excludes |

---

### Tests

Located in `AnalyzerUnmappedTests`. All queries use `setUnmappedLoad(...)`.
Helper to extract the attribute from a plan:

```java
private static UnmappedFieldsPattern patternFor(LogicalPlan plan) {
    EsRelation esr = plan.collect(EsRelation.class).get(0);
    return esr.output().stream()
        .filter(a -> a instanceof UnmappedFieldsAttribute)
        .map(a -> ((UnmappedFieldsAttribute) a).pattern())
        .findFirst()
        .orElseThrow();
}
```

| Query | Expected pattern |
|---|---|
| `FROM test` | `ALL` |
| `FROM test \| KEEP *` | `ALL` |
| `FROM test \| KEEP first_name*` | `{includes=[first_name*]}` |
| `FROM test \| KEEP salary` | `{includes=[salary]}` |
| `FROM test \| KEEP first_name*, salary` | `{includes=[first_name*, salary]}` |
| `FROM test \| DROP salary` | `{includes=[*], excludes=[salary]}` |
| `FROM test \| RENAME last_name AS x` | `{includes=[*], excludes=[x]}` |
| `FROM test \| KEEP first_name* \| EVAL z = 1` | `{includes=[first_name*], excludes=[z]}` |
| `FROM test \| EVAL z = 1 \| KEEP first_name*` | `{includes=[first_name*], excludes=[z]}` |
| `FROM test \| DROP salary \| RENAME last_name AS x` | `{includes=[*], excludes=[x, salary]}` |

`UnmappedFieldsToKeepTests.java` (old pre-analysis unit tests) → deleted.

---

## Checklist

### 0. Permissions ✅
- [x] All permissions granted

### 1. `UnmappedFieldsPattern` record ✅
- [x] Created; sentinels `ALL`, `NONE`; canonical constructor copies lists

### 2. `unmappedFieldsToKeep()` chain ✅
- [x] `LogicalPlan` stub
- [x] `UnaryPlan` pass-through
- [x] `EsRelation` → `ALL`
- [x] `ResolvingProject` → computes from own pattern + child
- [x] `Eval` → adds computed fields to child's excludes

### 3. Make `MetadataAttribute` sealed
- [ ] Remove `final`; add `sealed ... permits UnmappedFieldsAttribute`

### 4. Create `UnmappedFieldsAttribute`
- [ ] `extends MetadataAttribute`; name `_unmapped_fields`; holds `UnmappedFieldsPattern`
- [ ] Not serialized

### 5. `EsRelation` changes
- [ ] Remove `unmappedFieldsPattern` raw field + accessors + `withUnmappedFieldsPattern()`
- [ ] Restore original 7-arg constructor as the only public one
- [ ] Verify `equals()`/`hashCode()` unchanged (attrs already compared)

### 6. `ResolvingProject` — always pass through `UnmappedFieldsAttribute`
- [ ] Add `projectionsWithUnmapped()` helper in the public constructor

### 7. `DetermineUnmappedFieldsToKeep` rule
- [ ] Registered before `ResolvedProjects` in Finish Analysis batch
- [ ] Computes pattern; annotates each non-LOOKUP `EsRelation`

### 8. Tests in `AnalyzerUnmappedTests`
- [ ] Delete `UnmappedFieldsToKeepTests.java`
- [ ] Add 10 test methods (see table above) using `patternFor()` helper
- [ ] All pass

### 9. `EsqlNodeSubclassTests` fixup
- [ ] `UnmappedFieldsAttribute` must be constructable by the test framework
      (add special case if needed — `UnmappedFieldsPattern` is a record, already handled)

### 10. Final checks
- [ ] No regressions in `AnalyzerTests`, `AnalyzerUnmappedTests`, optimizer / physical plan tests
- [ ] `spotlessApply` clean
- [ ] Javadoc on new public methods / classes
