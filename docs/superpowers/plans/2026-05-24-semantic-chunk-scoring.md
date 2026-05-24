# Per-Chunk Scoring for semantic_text — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `min_score` and `chunks_per_doc` parameters to `semantic_query` that return per-chunk scores, text, and offsets in the search response.

**Architecture:** `SemanticQueryBuilder` gains two optional params parsed from XContent and serialized for transport. During query rewrite, it registers a `SearchExtBuilder` containing the chunk config. A new `SemanticChunksFetchSubPhase` (registered via `InferencePlugin.getFetchSubPhases()`) retrieves the config from `FetchContext.getSearchExt()`, uses the existing `SemanticChunkScorer` to score chunks per hit, and attaches results to `SearchHit` via a new `_chunks` field.

**Tech Stack:** Java 25, Elasticsearch Gradle build, x-pack inference plugin, ESTestCase test framework.

**Spec:** `docs/superpowers/specs/2026-05-24-semantic-chunk-scoring-design.md`

---

## File Map

### New Files

| File | Responsibility |
|------|---------------|
| `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/queries/SemanticChunksExtBuilder.java` | `SearchExtBuilder` that carries `minScore` (Float) and `chunksPerDoc` (Integer) from query rewrite to the fetch phase. |
| `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/search/SemanticChunksFetchSubPhase.java` | `FetchSubPhase` implementation that scores chunks per hit using `SemanticChunkScorer` and attaches `_chunks` to `SearchHit`. |
| `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/search/ChunkResult.java` | Record holding per-chunk result data: `text`, `startOffset`, `endOffset`, `score`. Implements `ToXContentObject` and `Writeable`. |
| `x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/queries/SemanticQueryBuilderChunksTests.java` | Unit tests for chunk parameters on SemanticQueryBuilder (parsing, serialization, validation). |
| `x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/search/SemanticChunksFetchSubPhaseTests.java` | Unit tests for the fetch sub-phase logic (scoring, filtering, ordering). |
| `x-pack/plugin/inference/src/internalClusterTest/java/org/elasticsearch/xpack/inference/search/SemanticChunkScoringIT.java` | Integration tests for the full end-to-end flow. |

### Modified Files

| File | Changes |
|------|---------|
| `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/queries/SemanticQueryBuilder.java` | Add `minScore` and `chunksPerDoc` fields, parse from XContent, serialize for transport, register `SemanticChunksExtBuilder` during rewrite. |
| `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/InferencePlugin.java` | Register `SemanticChunksFetchSubPhase` via `getFetchSubPhases()`, register `SemanticChunksExtBuilder` via `getSearchExts()`. |
| `server/src/main/java/org/elasticsearch/search/SearchHit.java` | Add `_chunks` field (`Map<String, List<ChunkResult>>`), getter/setter, serialization, XContent output. |

---

## Task 1: ChunkResult Record

**Files:**
- Create: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/search/ChunkResult.java`

- [ ] **Step 1: Create ChunkResult record**

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Represents a single scored chunk from a semantic_text field.
 * Returned in the {@code _chunks} section of search hits when
 * {@code min_score} or {@code chunks_per_doc} is set on a semantic query.
 */
public record ChunkResult(String text, int startOffset, int endOffset, float score) implements ToXContentObject, Writeable {

    public ChunkResult(StreamInput in) throws IOException {
        this(in.readString(), in.readVInt(), in.readVInt(), in.readFloat());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(text);
        out.writeVInt(startOffset);
        out.writeVInt(endOffset);
        out.writeFloat(score);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("text", text);
        builder.field("start_offset", startOffset);
        builder.field("end_offset", endOffset);
        builder.field("score", score);
        builder.endObject();
        return builder;
    }
}
```

- [ ] **Step 2: Compile check**

Run: `./gradlew :x-pack:plugin:inference:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Run spotless and commit**

```bash
./gradlew :x-pack:plugin:inference:spotlessApply
git add x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/search/ChunkResult.java
git commit -m "Add ChunkResult record for per-chunk scoring"
```

---

## Task 2: SearchHit _chunks Field

**Files:**
- Modify: `server/src/main/java/org/elasticsearch/search/SearchHit.java`

This task adds the `_chunks` field to `SearchHit` following the exact same pattern as `innerHits` and `highlightFields`.

- [ ] **Step 1: Read SearchHit.java to understand field patterns**

Read the full file. Key locations:
- Field declarations (~line 93-116)
- Constructor (~line 157-199)
- readFrom() (~line 201-282)
- writeTo() (~line 300-337)
- Getter/setter methods (~line 580-700)
- toXContent() (~line 853-965)
- Fields constants inner class (~line 970+)

- [ ] **Step 2: Add _chunks field**

Add the following to `SearchHit.java`. Follow the `highlightFields` pattern exactly:

1. **Field declaration** (near line 96, after highlightFields):
```java
private Map<String, List<ChunkResult>> chunks;
```

Note: `ChunkResult` is in the inference plugin (x-pack), but `SearchHit` is in `server`. This means `SearchHit` cannot reference `ChunkResult` directly. Instead, use a generic approach:

```java
private Map<String, List<Map<String, Object>>> chunks;
```

Or better: create a `ChunkResult`-like record in the `server` module. However, to avoid cross-module dependencies, the cleanest approach is:

**Option: Store chunks as a raw Map and serialize generically.**

Actually, look at how `innerHits` stores `SearchHits` (which is in the server module). The pattern is:
- The field uses types available in the `server` module
- The fetch sub-phase constructs the data and sets it

Since `ChunkResult` needs to be in the server module for `SearchHit` to reference it, move it there:

Create `server/src/main/java/org/elasticsearch/search/fetch/subphase/ChunkResult.java` instead. Then `SearchHit` can reference it directly.

Alternatively, store as `Map<String, List<ToXContent>>` and handle serialization generically.

**Recommended approach:** Add `ChunkResult` to the server module since it's a simple data record with no inference-specific dependencies.

2. **Add field constant** (in the Fields inner class):
```java
static final String CHUNKS = "_chunks";
```

3. **Add getter/setter**:
```java
public Map<String, List<ChunkResult>> getChunks() {
    return chunks == null ? Map.of() : chunks;
}

public void setChunks(Map<String, List<ChunkResult>> chunks) {
    this.chunks = chunks;
}
```

4. **Add to writeTo()** (after highlight serialization):
```java
if (chunks == null) {
    out.writeVInt(0);
} else {
    out.writeVInt(chunks.size());
    for (Map.Entry<String, List<ChunkResult>> entry : chunks.entrySet()) {
        out.writeString(entry.getKey());
        out.writeCollection(entry.getValue());
    }
}
```

5. **Add to readFrom()** (after highlight deserialization):
```java
int chunksSize = in.readVInt();
if (chunksSize > 0) {
    chunks = Maps.newMapWithExpectedSize(chunksSize);
    for (int i = 0; i < chunksSize; i++) {
        String key = in.readString();
        List<ChunkResult> chunkList = in.readCollectionAsList(ChunkResult::new);
        chunks.put(key, chunkList);
    }
}
```

6. **Add to toXContent()** (after highlight output, before inner_hits):
```java
if (chunks != null && chunks.isEmpty() == false) {
    builder.startObject(Fields.CHUNKS);
    for (Map.Entry<String, List<ChunkResult>> entry : chunks.entrySet()) {
        builder.startArray(entry.getKey());
        for (ChunkResult chunk : entry.getValue()) {
            chunk.toXContent(builder, params);
        }
        builder.endArray();
    }
    builder.endObject();
}
```

7. **Add to doEquals/doHashCode** if they exist.

- [ ] **Step 3: Move ChunkResult to server module**

Move `ChunkResult.java` from `x-pack/plugin/inference/src/main/java/.../search/ChunkResult.java` to `server/src/main/java/org/elasticsearch/search/fetch/subphase/ChunkResult.java`. Update the package declaration.

- [ ] **Step 4: Compile check**

Run: `./gradlew :server:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 5: Run spotless and commit**

```bash
./gradlew :server:spotlessApply
git add server/src/main/java/org/elasticsearch/search/SearchHit.java \
        server/src/main/java/org/elasticsearch/search/fetch/subphase/ChunkResult.java
git commit -m "Add _chunks field to SearchHit for per-chunk scoring"
```

---

## Task 3: SemanticChunksExtBuilder

**Files:**
- Create: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/queries/SemanticChunksExtBuilder.java`

This `SearchExtBuilder` carries chunk parameters from query rewrite to the fetch phase via `SearchContext.getSearchExt()`.

- [ ] **Step 1: Implement SemanticChunksExtBuilder**

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Carries per-chunk scoring configuration from {@link SemanticQueryBuilder} to
 * {@link org.elasticsearch.xpack.inference.search.SemanticChunksFetchSubPhase}
 * via the search ext mechanism.
 */
public class SemanticChunksExtBuilder extends SearchExtBuilder {

    public static final String NAME = "semantic_chunks";

    private final String fieldName;
    @Nullable
    private final Float minScore;
    @Nullable
    private final Integer chunksPerDoc;

    public SemanticChunksExtBuilder(String fieldName, @Nullable Float minScore, @Nullable Integer chunksPerDoc) {
        this.fieldName = Objects.requireNonNull(fieldName);
        this.minScore = minScore;
        this.chunksPerDoc = chunksPerDoc;
    }

    public SemanticChunksExtBuilder(StreamInput in) throws IOException {
        this.fieldName = in.readString();
        this.minScore = in.readOptionalFloat();
        this.chunksPerDoc = in.readOptionalInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeOptionalFloat(minScore);
        out.writeOptionalInt(chunksPerDoc);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Nullable
    public Float getMinScore() {
        return minScore;
    }

    @Nullable
    public Integer getChunksPerDoc() {
        return chunksPerDoc;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("field", fieldName);
        if (minScore != null) {
            builder.field("min_score", minScore);
        }
        if (chunksPerDoc != null) {
            builder.field("chunks_per_doc", chunksPerDoc);
        }
        builder.endObject();
        return builder;
    }

    public static SemanticChunksExtBuilder fromXContent(XContentParser parser) throws IOException {
        // Parsing from ext section — not typically used from user input
        // but needed for transport serialization round-trip
        String fieldName = null;
        Float minScore = null;
        Integer chunksPerDoc = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("field".equals(currentFieldName)) {
                    fieldName = parser.text();
                } else if ("min_score".equals(currentFieldName)) {
                    minScore = parser.floatValue();
                } else if ("chunks_per_doc".equals(currentFieldName)) {
                    chunksPerDoc = parser.intValue();
                }
            }
        }
        return new SemanticChunksExtBuilder(fieldName, minScore, chunksPerDoc);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SemanticChunksExtBuilder that = (SemanticChunksExtBuilder) o;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.equals(minScore, that.minScore)
            && Objects.equals(chunksPerDoc, that.chunksPerDoc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, minScore, chunksPerDoc);
    }
}
```

- [ ] **Step 2: Compile check**

Run: `./gradlew :x-pack:plugin:inference:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Run spotless and commit**

```bash
./gradlew :x-pack:plugin:inference:spotlessApply
git add x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/queries/SemanticChunksExtBuilder.java
git commit -m "Add SemanticChunksExtBuilder for chunk config transport"
```

---

## Task 4: SemanticQueryBuilder — Add Chunk Parameters

**Files:**
- Modify: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/queries/SemanticQueryBuilder.java`
- Create: `x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/queries/SemanticQueryBuilderChunksTests.java`

- [ ] **Step 1: Write failing tests for chunk parameter parsing**

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

public class SemanticQueryBuilderChunksTests extends ESTestCase {

    public void testParseWithChunksPerDoc() throws IOException {
        String json = """
            { "field": "content", "query": "test", "chunks_per_doc": 5 }
            """;
        SemanticQueryBuilder builder = parseQuery(json);
        assertThat(builder.chunksPerDoc(), equalTo(5));
        assertNull(builder.minScore());
    }

    public void testParseWithMinScore() throws IOException {
        String json = """
            { "field": "content", "query": "test", "min_score": 0.7 }
            """;
        SemanticQueryBuilder builder = parseQuery(json);
        assertThat(builder.minScore(), equalTo(0.7f));
        assertNull(builder.chunksPerDoc());
    }

    public void testParseWithBothParams() throws IOException {
        String json = """
            { "field": "content", "query": "test", "min_score": 0.5, "chunks_per_doc": 3 }
            """;
        SemanticQueryBuilder builder = parseQuery(json);
        assertThat(builder.minScore(), equalTo(0.5f));
        assertThat(builder.chunksPerDoc(), equalTo(3));
    }

    public void testParseWithoutChunkParams() throws IOException {
        String json = """
            { "field": "content", "query": "test" }
            """;
        SemanticQueryBuilder builder = parseQuery(json);
        assertNull(builder.minScore());
        assertNull(builder.chunksPerDoc());
    }

    public void testChunksPerDocMustBePositive() throws IOException {
        String json = """
            { "field": "content", "query": "test", "chunks_per_doc": 0 }
            """;
        expectThrows(IllegalArgumentException.class, () -> parseQuery(json));
    }

    public void testMinScoreMustBeNonNegative() throws IOException {
        String json = """
            { "field": "content", "query": "test", "min_score": -0.1 }
            """;
        expectThrows(IllegalArgumentException.class, () -> parseQuery(json));
    }

    public void testHasChunkConfig() throws IOException {
        SemanticQueryBuilder noChunks = new SemanticQueryBuilder("field", "query");
        assertFalse(noChunks.hasChunkConfig());

        // After implementing: builders with min_score or chunks_per_doc should return true
    }

    private SemanticQueryBuilder parseQuery(String json) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray(json))) {
            return SemanticQueryBuilder.fromXContent(parser);
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.queries.SemanticQueryBuilderChunksTests" -Dtests.iters=1`
Expected: Compilation failure — `chunksPerDoc()`, `minScore()`, `hasChunkConfig()` do not exist.

- [ ] **Step 3: Add chunk parameters to SemanticQueryBuilder**

In `SemanticQueryBuilder.java`, make these changes:

1. **Add field constants** (near other ParseField declarations):
```java
private static final ParseField MIN_SCORE_FIELD = new ParseField("min_score");
private static final ParseField CHUNKS_PER_DOC_FIELD = new ParseField("chunks_per_doc");
```

2. **Add fields** (near other instance fields):
```java
@Nullable
private final Float minScore;
@Nullable
private final Integer chunksPerDoc;
```

3. **Update constructors** — add `minScore` and `chunksPerDoc` parameters to the internal constructors. The public constructors default both to null. Add validation: `chunksPerDoc >= 1` if not null, `minScore >= 0` if not null.

4. **Update PARSER** — add declarations:
```java
PARSER.declareFloat(optionalConstructorArg(), MIN_SCORE_FIELD);
PARSER.declareInt(optionalConstructorArg(), CHUNKS_PER_DOC_FIELD);
```

Note: this changes the number of constructor args in the `ConstructingObjectParser`. Update the lambda accordingly.

5. **Add getters**:
```java
@Nullable
public Float minScore() { return minScore; }

@Nullable
public Integer chunksPerDoc() { return chunksPerDoc; }

public boolean hasChunkConfig() { return minScore != null || chunksPerDoc != null; }
```

6. **Update doXContent()** to serialize new fields:
```java
if (minScore != null) {
    builder.field(MIN_SCORE_FIELD.getPreferredName(), minScore);
}
if (chunksPerDoc != null) {
    builder.field(CHUNKS_PER_DOC_FIELD.getPreferredName(), chunksPerDoc);
}
```

7. **Update writeTo() and StreamInput constructor** for transport serialization. Use a new `TransportVersion` constant for backward compatibility:
```java
// In writeTo:
if (out.getTransportVersion().onOrAfter(SEMANTIC_QUERY_CHUNK_SCORING)) {
    out.writeOptionalFloat(minScore);
    out.writeOptionalInt(chunksPerDoc);
}

// In StreamInput constructor:
if (in.getTransportVersion().onOrAfter(SEMANTIC_QUERY_CHUNK_SCORING)) {
    this.minScore = in.readOptionalFloat();
    this.chunksPerDoc = in.readOptionalInt();
} else {
    this.minScore = null;
    this.chunksPerDoc = null;
}
```

8. **Update doEquals() and doHashCode()** to include the new fields.

9. **Register the new TransportVersion** — add a constant in the same pattern as other transport versions in the file. Generate with `./gradlew generateTransportVersion`.

- [ ] **Step 4: Register SemanticChunksExtBuilder in rewrite**

In the `doRewriteBuildSemanticQuery()` method, after building the semantic query, if `hasChunkConfig()` is true, register a `SemanticChunksExtBuilder`:

```java
if (hasChunkConfig()) {
    // Register ext builder so the fetch sub-phase can access chunk config
    context.registerSearchExt(new SemanticChunksExtBuilder(fieldName, minScore, chunksPerDoc));
}
```

Check how `context.registerSearchExt()` works — it may be on `QueryRewriteContext` or `SearchExecutionContext`. Read the available methods and adapt. If `registerSearchExt` doesn't exist on the rewrite context, an alternative is to store the config on the `SearchContext` via a different mechanism. Consult how other features pass data from query to fetch phase.

- [ ] **Step 5: Run tests to verify they pass**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.queries.SemanticQueryBuilderChunksTests" -Dtests.iters=1`
Expected: All tests PASS.

- [ ] **Step 6: Run existing SemanticQueryBuilder tests**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.queries.SemanticQueryBuilderTests" -Dtests.iters=1`
Expected: All existing tests still PASS.

- [ ] **Step 7: Run spotless and commit**

```bash
./gradlew :x-pack:plugin:inference:spotlessApply
git add x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/queries/SemanticQueryBuilder.java \
        x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/queries/SemanticQueryBuilderChunksTests.java
git commit -m "Add min_score and chunks_per_doc to SemanticQueryBuilder"
```

---

## Task 5: SemanticChunksFetchSubPhase

**Files:**
- Create: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/search/SemanticChunksFetchSubPhase.java`
- Create: `x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/search/SemanticChunksFetchSubPhaseTests.java`

- [ ] **Step 1: Write failing tests**

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.search;

import org.elasticsearch.test.ESTestCase;

public class SemanticChunksFetchSubPhaseTests extends ESTestCase {

    // Test that getProcessor returns null when no chunk config is in the search context
    public void testNoProcessorWhenNoChunkConfig() throws Exception {
        // Create mock FetchContext with no SemanticChunksExtBuilder
        // Verify getProcessor() returns null
    }

    // Test that getProcessor returns a processor when chunk config exists
    public void testProcessorCreatedWhenChunkConfigExists() throws Exception {
        // Create mock FetchContext with SemanticChunksExtBuilder
        // Verify getProcessor() returns non-null
    }

    // Test filtering by min_score
    public void testMinScoreFiltering() {
        // Given chunks with scores [0.9, 0.7, 0.5, 0.3]
        // When min_score = 0.6
        // Then only chunks with scores [0.9, 0.7] are returned
    }

    // Test limiting by chunks_per_doc
    public void testChunksPerDocLimit() {
        // Given chunks with scores [0.9, 0.7, 0.5, 0.3]
        // When chunks_per_doc = 2
        // Then only [0.9, 0.7] returned
    }

    // Test both min_score and chunks_per_doc together
    public void testMinScoreAndChunksPerDoc() {
        // Given chunks with scores [0.9, 0.7, 0.5, 0.3]
        // When min_score = 0.4, chunks_per_doc = 2
        // Then [0.9, 0.7] (3 pass min_score, but capped to 2)
    }

    // Test chunks_per_doc only (no min_score)
    public void testChunksPerDocOnly() {
        // Given chunks with scores [0.9, 0.7, 0.5, 0.3]
        // When chunks_per_doc = 3, no min_score
        // Then [0.9, 0.7, 0.5] returned
    }

    // Test min_score only (no chunks_per_doc cap)
    public void testMinScoreOnly() {
        // Given chunks with scores [0.9, 0.7, 0.5, 0.3]
        // When min_score = 0.4, no chunks_per_doc
        // Then [0.9, 0.7, 0.5] returned (all above 0.4)
    }

    // Test no chunks above min_score returns empty
    public void testNoChunksAboveMinScore() {
        // Given chunks with scores [0.3, 0.2, 0.1]
        // When min_score = 0.5
        // Then empty list
    }

    // Test score ordering (descending by score, ties broken by start_offset ascending)
    public void testScoreOrdering() {
        // Given chunks with scores [0.7 at offset 100, 0.9 at offset 0, 0.7 at offset 50]
        // Then ordered: [0.9@0, 0.7@50, 0.7@100]
    }

    // Test chunks_per_doc = 1 returns only best
    public void testChunksPerDocOne() {
        // Given chunks with scores [0.9, 0.7, 0.5]
        // When chunks_per_doc = 1
        // Then only [0.9]
    }

    // Test min_score = 0.0 returns all chunks
    public void testMinScoreZero() {
        // Given chunks with scores [0.9, 0.7, 0.5, 0.3]
        // When min_score = 0.0
        // Then all 4 returned
    }

    // Test single chunk covering entire text
    public void testSingleChunk() {
        // Given 1 chunk with score 0.8
        // When chunks_per_doc = 5
        // Then [0.8] (only 1 chunk available)
    }
}
```

Note: The test bodies above are descriptions — the implementing engineer needs to fill in actual mock/test infrastructure. The key pattern is: create `ChunkResult` objects with known scores, apply the filtering/sorting logic, and assert the output. If the filtering logic is extracted to a static helper method on `SemanticChunksFetchSubPhase`, these tests can call it directly without needing full `FetchContext` mocking.

- [ ] **Step 2: Implement SemanticChunksFetchSubPhase**

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.search;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.fetch.subphase.ChunkResult;
import org.elasticsearch.xpack.core.common.chunks.ScoredChunk;
import org.elasticsearch.xpack.inference.common.chunks.SemanticChunkScorer;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.queries.SemanticChunksExtBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Fetch sub-phase that scores individual chunks of semantic_text fields
 * and attaches them as {@code _chunks} on each {@link SearchHit}.
 *
 * Activated when a semantic query has {@code min_score} or {@code chunks_per_doc} set.
 * Uses {@link SemanticChunkScorer} (already used by highlighting and ES|QL)
 * to compute per-chunk similarity scores.
 */
public class SemanticChunksFetchSubPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) throws IOException {
        SemanticChunksExtBuilder ext = (SemanticChunksExtBuilder) fetchContext.getSearchExt(SemanticChunksExtBuilder.NAME);
        if (ext == null) {
            return null;
        }

        String fieldName = ext.getFieldName();
        Float minScore = ext.getMinScore();
        Integer chunksPerDoc = ext.getChunksPerDoc();

        // Resolve the semantic text field type
        var mappingLookup = fetchContext.getSearchExecutionContext().getMappingLookup();
        var fieldType = mappingLookup.getFieldType(fieldName);
        if (fieldType instanceof SemanticTextFieldMapper.SemanticTextFieldType == false) {
            return null;
        }
        SemanticTextFieldMapper.SemanticTextFieldType semanticFieldType =
            (SemanticTextFieldMapper.SemanticTextFieldType) fieldType;

        // Create the chunk scorer using the search context
        // Note: need to access SearchContext — check if FetchContext exposes it
        // or if we can construct SemanticChunkScorer from available FetchContext data

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(org.apache.lucene.index.LeafReaderContext readerContext) {
                // No per-reader state needed — SemanticChunkScorer handles this internally
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NEEDS_SOURCE;
            }

            @Override
            public void process(FetchSubPhase.HitContext hitContext) throws IOException {
                SearchHit hit = hitContext.hit();

                // Score chunks using SemanticChunkScorer
                // The scorer needs SearchContext — get it from FetchContext
                // Pass Integer.MAX_VALUE as maxResults to get all chunks, then filter ourselves
                List<ScoredChunk> scoredChunks = scoreChunksForHit(semanticFieldType, hit);

                // Apply min_score filter
                List<ChunkResult> results = new ArrayList<>();
                for (ScoredChunk chunk : scoredChunks) {
                    if (minScore != null && chunk.score() < minScore) {
                        continue;
                    }
                    results.add(new ChunkResult(
                        chunk.content(),
                        // Extract offsets — ScoredChunk may not have them directly
                        // May need to use OffsetAndScore instead of ScoredChunk
                        0, 0, // placeholder — see implementation note below
                        chunk.score()
                    ));
                }

                // Sort by score desc, then start_offset asc for ties
                results.sort(Comparator.comparingDouble(ChunkResult::score).reversed()
                    .thenComparingInt(ChunkResult::startOffset));

                // Apply chunks_per_doc cap
                if (chunksPerDoc != null && results.size() > chunksPerDoc) {
                    results = results.subList(0, chunksPerDoc);
                }

                // Attach to hit
                Map<String, List<ChunkResult>> chunksMap = new HashMap<>();
                chunksMap.put(fieldName, results);
                hit.setChunks(chunksMap);
            }
        };
    }
}
```

**Implementation note on offsets:** `ScoredChunk` record has `(String content, float score, int originalIndex)` but not `startOffset`/`endOffset`. The fetch sub-phase should use `SemanticTextChunkUtils.extractOffsetAndScores()` directly (like `SemanticTextHighlighter` does) instead of `SemanticChunkScorer.scoreChunks()`, since `OffsetAndScore` has the offset information. Read `SemanticTextHighlighter.java` lines 73-131 to see the exact pattern for extracting offsets and scores. Follow that pattern rather than going through `SemanticChunkScorer`.

- [ ] **Step 3: Fill in test method bodies**

Use the static filtering/sorting helper (extract if needed) to unit test the logic without full FetchContext mocking. For the processor tests that need FetchContext, mock the minimal surface.

- [ ] **Step 4: Run tests**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.search.SemanticChunksFetchSubPhaseTests" -Dtests.iters=1`
Expected: All tests PASS.

- [ ] **Step 5: Run spotless and commit**

```bash
./gradlew :x-pack:plugin:inference:spotlessApply
git add x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/search/SemanticChunksFetchSubPhase.java \
        x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/search/SemanticChunksFetchSubPhaseTests.java
git commit -m "Add SemanticChunksFetchSubPhase for per-chunk scoring"
```

---

## Task 6: Register in InferencePlugin

**Files:**
- Modify: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/InferencePlugin.java`

- [ ] **Step 1: Register the fetch sub-phase and search ext**

In `InferencePlugin.java`:

1. **Add `getFetchSubPhases()` override** (if not already present):
```java
@Override
public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
    return List.of(new SemanticChunksFetchSubPhase());
}
```

2. **Add `getSearchExts()` override** to register the ext builder parser:
```java
@Override
public List<SearchExtSpec<?>> getSearchExts() {
    return List.of(
        new SearchExtSpec<>(
            SemanticChunksExtBuilder.NAME,
            SemanticChunksExtBuilder::new,
            SemanticChunksExtBuilder::fromXContent
        )
    );
}
```

Read InferencePlugin.java to check if these methods already exist — if so, add to the existing lists.

- [ ] **Step 2: Compile check**

Run: `./gradlew :x-pack:plugin:inference:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Run spotless and commit**

```bash
./gradlew :x-pack:plugin:inference:spotlessApply
git add x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/InferencePlugin.java
git commit -m "Register SemanticChunksFetchSubPhase in InferencePlugin"
```

---

## Task 7: Integration Test

**Files:**
- Create: `x-pack/plugin/inference/src/internalClusterTest/java/org/elasticsearch/xpack/inference/search/SemanticChunkScoringIT.java`

- [ ] **Step 1: Write integration test**

Follow the pattern from `ShardBulkInferenceActionFilterIT.java` for setting up the test infrastructure (inference endpoints, index creation, document indexing).

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.search;

/**
 * Integration tests for per-chunk scoring in semantic_text queries.
 */
public class SemanticChunkScoringIT extends ... {

    /**
     * Test chunks_per_doc only: index a document with multiple chunks,
     * query with chunks_per_doc=2, verify response has _chunks with 2 entries.
     */
    public void testChunksPerDocOnly() throws Exception {
        // 1. Create inference endpoint
        // 2. Create index with semantic_text field
        // 3. Index document with text that produces multiple chunks
        // 4. Search with: { "semantic": { "field": "content", "query": "test", "chunks_per_doc": 2 } }
        // 5. Verify response has _chunks.content with exactly 2 entries
        // 6. Verify each chunk has text, start_offset, end_offset, score
        // 7. Verify chunks are sorted by score descending
    }

    /**
     * Test min_score only: verify only qualifying chunks returned.
     */
    public void testMinScoreOnly() throws Exception {
        // Similar setup
        // Search with min_score that filters some chunks
        // Verify only qualifying chunks returned
    }

    /**
     * Test both parameters together.
     */
    public void testMinScoreAndChunksPerDoc() throws Exception {
        // Combine both filters
    }

    /**
     * Test no chunk params: verify response is unchanged from today.
     */
    public void testNoChunkParamsBackwardCompat() throws Exception {
        // Search without min_score or chunks_per_doc
        // Verify no _chunks in response
    }

    /**
     * Test min_score filtering out all chunks: document still in hits,
     * _chunks is empty array.
     */
    public void testMinScoreFiltersAllChunks() throws Exception {
        // Use very high min_score
        // Verify document appears but _chunks.content is empty
    }
}
```

- [ ] **Step 2: Run integration tests**

Run: `./gradlew :x-pack:plugin:inference:internalClusterTest --tests "org.elasticsearch.xpack.inference.search.SemanticChunkScoringIT" -Dtests.iters=1`
Expected: All tests PASS.

- [ ] **Step 3: Run spotless and commit**

```bash
./gradlew :x-pack:plugin:inference:spotlessApply
git add x-pack/plugin/inference/src/internalClusterTest/java/org/elasticsearch/xpack/inference/search/SemanticChunkScoringIT.java
git commit -m "Add integration tests for per-chunk scoring"
```

---

## Task 8: Final Verification

- [ ] **Step 1: Run precommit checks**

Run: `./gradlew :x-pack:plugin:inference:precommit`
Expected: PASS

- [ ] **Step 2: Run all inference plugin tests**

Run: `./gradlew :x-pack:plugin:inference:test -Dtests.iters=1`
Expected: PASS

- [ ] **Step 3: Run server precommit (for SearchHit changes)**

Run: `./gradlew :server:precommit`
Expected: PASS

- [ ] **Step 4: Run integration tests**

Run: `./gradlew :x-pack:plugin:inference:internalClusterTest -Dtests.iters=1`
Expected: PASS

---

## Reference: Key Files and Line Numbers

| File | Key Lines | What's There |
|------|-----------|-------------|
| `SemanticQueryBuilder.java` | 68-79 | PARSER setup |
| `SemanticQueryBuilder.java` | 81-92 | Field declarations |
| `SemanticQueryBuilder.java` | 132-160 | StreamInput constructor |
| `SemanticQueryBuilder.java` | 163-203 | writeTo() |
| `SemanticQueryBuilder.java` | 280-289 | doXContent() |
| `SemanticQueryBuilder.java` | 292-304 | doRewrite() |
| `SemanticQueryBuilder.java` | 415-426 | doEquals/doHashCode |
| `SemanticFieldMapper.java` | 938-1005 | semanticQuery() method |
| `SemanticChunkScorer.java` | 51-139 | scoreChunks() method |
| `SemanticTextHighlighter.java` | 73-131 | Chunk scoring pattern (use as template) |
| `SemanticTextChunkUtils.java` | 93-158 | extractQueries() and extractOffsetAndScores() |
| `SearchHit.java` | 93-116 | Field declarations |
| `SearchHit.java` | 201-282 | readFrom() serialization |
| `SearchHit.java` | 300-337 | writeTo() serialization |
| `SearchHit.java` | 853-965 | toXContent() output |
| `FetchSubPhase.java` | 27-105 | Interface definition |
| `FetchSubPhaseProcessor.java` | 18-45 | Processor interface |
| `FetchScorePhase.java` | 25-60 | Simple FetchSubPhase example |
| `HighlightPhase.java` | 48-81 | Complex FetchSubPhase example |
| `FetchContext.java` | 255-256 | getSearchExt() method |
| `InferencePlugin.java` | varies | Plugin registration point |
| `ScoredChunk.java` | record | `(String content, float score, int originalIndex)` |
