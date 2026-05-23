# BYO Chunks and Vectors for semantic_text — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow semantic_text fields to accept precomputed chunks and embeddings via a multi-part staging protocol, bypassing inference at index time.

**Architecture:** Extend `ShardBulkInferenceActionFilter` to detect structured BYO objects in semantic_text fields and route them through new staging/commit logic. Staged data lives in `_inference_metadata.<field>._staged`. A new index setting (`index.semantic_text.staged_ttl`) and REST endpoint (`_semantic_cleanup`) handle stale staged data cleanup.

**Tech Stack:** Java 25, Elasticsearch Gradle build, x-pack inference plugin, ESTestCase test framework.

**Spec:** `docs/superpowers/specs/2026-05-23-byo-chunks-vectors-semantic-text-design.md`

---

## File Map

### New Files

| File | Responsibility |
|------|---------------|
| `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/StagedSemanticField.java` | Record representing staged BYO data: text, textLength, lastModified, chunks. Serialization/deserialization to/from XContent. |
| `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticAction.java` | Enum for `_action` values: `stage_init`, `stage`, `commit`, `cancel`. Parsing from field value object. |
| `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticValidator.java` | Validation logic: per-chunk validation (bounds, offsets), cross-chunk overlap detection, commit-time gap/coverage checking, dimensional compatibility. |
| `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticHandler.java` | Orchestrator called from `ShardBulkInferenceActionFilter`: detects BYO objects, dispatches to stage_init/stage/commit/cancel/single-shot handlers, reads/writes `_staged` in `_inference_metadata`. |
| `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/action/StagedSemanticCleanupAction.java` | ActionType for the `_semantic_cleanup` endpoint. |
| `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/action/StagedSemanticCleanupRequest.java` | Request class: index, optional field, optional max_age override. |
| `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/action/TransportStagedSemanticCleanupAction.java` | Transport action: scans shards for documents with expired `_staged`, clears them. |
| `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/rest/RestStagedSemanticCleanupAction.java` | REST handler: `POST /{index}/_semantic_cleanup`. |
| `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/action/AsyncStagedSemanticCleanupTask.java` | Background periodic task extending `AbstractAsyncTask` for TTL-based cleanup. |
| `x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticValidatorTests.java` | Unit tests for all validation logic. |
| `x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/mapper/StagedSemanticFieldTests.java` | Unit tests for StagedSemanticField serialization. |
| `x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticHandlerTests.java` | Unit tests for BYO detection and dispatch logic. |
| `x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/action/filter/ShardBulkInferenceActionFilterBYOTests.java` | Integration-level unit tests for the full BYO flow through the filter. |

### Modified Files

| File | Changes |
|------|---------|
| `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/action/filter/ShardBulkInferenceActionFilter.java` | Add BYO detection in `addFieldInferenceRequests()`. When field value is a Map with `_action` or BYO shape, delegate to `BYOSemanticHandler` instead of creating inference requests. |
| `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/SemanticTextField.java` | Add `STAGED_FIELD = "_staged"` constant. Add `StagedSemanticField` integration in `toXContent` and parsing. |
| `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/InferencePlugin.java` | Register new index setting, REST action, transport action. |
| `server/src/main/java/org/elasticsearch/index/IndexSettings.java` | Add `INDEX_SEMANTIC_TEXT_STAGED_TTL` setting. |
| `server/src/main/java/org/elasticsearch/common/settings/IndexScopedSettings.java` | Register `INDEX_SEMANTIC_TEXT_STAGED_TTL` in `BUILT_IN_INDEX_SETTINGS`. |

---

## Task 1: BYOSemanticAction Enum and Parsing

**Files:**
- Create: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticAction.java`
- Test: `x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticHandlerTests.java` (first tests)

- [ ] **Step 1: Write failing tests for action parsing**

Create the test file. These tests verify that the enum parses correctly from strings and rejects unknown values.

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.test.ESTestCase;

public class BYOSemanticHandlerTests extends ESTestCase {

    public void testParseStageInit() {
        assertThat(BYOSemanticAction.fromString("stage_init"), equalTo(BYOSemanticAction.STAGE_INIT));
    }

    public void testParseStage() {
        assertThat(BYOSemanticAction.fromString("stage"), equalTo(BYOSemanticAction.STAGE));
    }

    public void testParseCommit() {
        assertThat(BYOSemanticAction.fromString("commit"), equalTo(BYOSemanticAction.COMMIT));
    }

    public void testParseCancel() {
        assertThat(BYOSemanticAction.fromString("cancel"), equalTo(BYOSemanticAction.CANCEL));
    }

    public void testParseUnknownActionThrows() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BYOSemanticAction.fromString("unknown")
        );
        assertThat(e.getMessage(), containsString("Unknown BYO semantic action [unknown]"));
    }

    public void testParseNullActionThrows() {
        expectThrows(NullPointerException.class, () -> BYOSemanticAction.fromString(null));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.mapper.BYOSemanticHandlerTests" -Dtests.iters=1`
Expected: Compilation failure — `BYOSemanticAction` does not exist.

- [ ] **Step 3: Implement BYOSemanticAction enum**

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import java.util.Locale;
import java.util.Objects;

/**
 * Enumerates the actions a user can specify in a BYO (bring-your-own) semantic_text field value
 * via the {@code _action} key. Each action corresponds to a phase of the multi-part upload protocol:
 * initialize staging, append chunks, commit (promote to searchable), or cancel.
 */
public enum BYOSemanticAction {
    STAGE_INIT,
    STAGE,
    COMMIT,
    CANCEL;

    private static final String ACTION_FIELD = "_action";

    /**
     * Parses an action string (e.g., "stage_init") into the corresponding enum value.
     *
     * @throws IllegalArgumentException if the string does not match any known action
     * @throws NullPointerException if value is null
     */
    public static BYOSemanticAction fromString(String value) {
        Objects.requireNonNull(value);
        try {
            return valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown BYO semantic action [" + value + "]. Valid actions: stage_init, stage, commit, cancel");
        }
    }

    /**
     * Returns the key used in the field value object to specify the action.
     */
    public static String actionField() {
        return ACTION_FIELD;
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.mapper.BYOSemanticHandlerTests" -Dtests.iters=1`
Expected: All 6 tests PASS.

- [ ] **Step 5: Run spotless and commit**

```bash
./gradlew :x-pack:plugin:inference:spotlessApply
git add x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticAction.java \
        x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticHandlerTests.java
git commit -m "Add BYOSemanticAction enum for multi-part upload protocol"
```

---

## Task 2: StagedSemanticField Record and Serialization

**Files:**
- Create: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/StagedSemanticField.java`
- Create: `x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/mapper/StagedSemanticFieldTests.java`

- [ ] **Step 1: Write failing tests for staged field serialization round-trip**

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

public class StagedSemanticFieldTests extends ESTestCase {

    public void testRoundTripEmptyChunks() throws IOException {
        StagedSemanticField staged = new StagedSemanticField(
            "hello world",
            11,
            Instant.parse("2026-05-23T14:30:00Z"),
            List.of()
        );
        StagedSemanticField parsed = roundTrip(staged);
        assertThat(parsed.text(), equalTo("hello world"));
        assertThat(parsed.textLength(), equalTo(11));
        assertThat(parsed.lastModified(), equalTo(Instant.parse("2026-05-23T14:30:00Z")));
        assertThat(parsed.chunks().size(), equalTo(0));
    }

    public void testRoundTripWithChunks() throws IOException {
        BytesReference embedding1 = new BytesArray("{\"values\":[0.1,0.2,0.3]}");
        BytesReference embedding2 = new BytesArray("{\"values\":[0.4,0.5,0.6]}");
        StagedSemanticField staged = new StagedSemanticField(
            "hello world foo bar",
            19,
            Instant.parse("2026-05-23T14:30:00Z"),
            List.of(
                new SemanticTextField.Chunk(0, 11, embedding1),
                new SemanticTextField.Chunk(11, 19, embedding2)
            )
        );
        StagedSemanticField parsed = roundTrip(staged);
        assertThat(parsed.text(), equalTo("hello world foo bar"));
        assertThat(parsed.textLength(), equalTo(19));
        assertThat(parsed.chunks().size(), equalTo(2));
        assertThat(parsed.chunks().get(0).startOffset(), equalTo(0));
        assertThat(parsed.chunks().get(0).endOffset(), equalTo(11));
        assertThat(parsed.chunks().get(1).startOffset(), equalTo(11));
        assertThat(parsed.chunks().get(1).endOffset(), equalTo(19));
    }

    public void testTextIsRequired() throws IOException {
        // Build JSON without "text" field
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("text_length", 5);
        builder.field("last_modified", "2026-05-23T14:30:00Z");
        builder.startArray("chunks");
        builder.endArray();
        builder.endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            expectThrows(Exception.class, () -> StagedSemanticField.fromXContent(parser));
        }
    }

    private StagedSemanticField roundTrip(StagedSemanticField original) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        original.toXContent(builder, EMPTY_PARAMS);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            return StagedSemanticField.fromXContent(parser);
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.mapper.StagedSemanticFieldTests" -Dtests.iters=1`
Expected: Compilation failure — `StagedSemanticField` does not exist.

- [ ] **Step 3: Implement StagedSemanticField record**

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Represents the staged (uncommitted) state of a BYO semantic_text field during multi-part upload.
 * Stored under {@code _inference_metadata.<field>._staged} and invisible to search queries
 * until committed.
 *
 * @param text         The full original text value, set at stage_init time.
 * @param textLength   The character length of the text, used for offset validation.
 * @param lastModified Timestamp of the last stage request, used for TTL-based cleanup.
 * @param chunks       The accumulated chunks with offsets and embeddings.
 */
public record StagedSemanticField(
    String text,
    int textLength,
    Instant lastModified,
    List<SemanticTextField.Chunk> chunks
) implements ToXContentObject {

    static final String TEXT_FIELD = "text";
    static final String TEXT_LENGTH_FIELD = "text_length";
    static final String LAST_MODIFIED_FIELD = "last_modified";
    static final String CHUNKS_FIELD = "chunks";
    static final String START_OFFSET_FIELD = "start_offset";
    static final String END_OFFSET_FIELD = "end_offset";
    static final String EMBEDDINGS_FIELD = "embeddings";

    public StagedSemanticField {
        Objects.requireNonNull(text, "text is required for staged semantic field");
        Objects.requireNonNull(lastModified, "lastModified is required");
        Objects.requireNonNull(chunks, "chunks list is required");
    }

    /**
     * Creates a new StagedSemanticField with additional chunks appended and an updated timestamp.
     */
    public StagedSemanticField withAppendedChunks(List<SemanticTextField.Chunk> newChunks, Instant newTimestamp) {
        List<SemanticTextField.Chunk> combined = new ArrayList<>(this.chunks);
        combined.addAll(newChunks);
        return new StagedSemanticField(text, textLength, newTimestamp, combined);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TEXT_FIELD, text);
        builder.field(TEXT_LENGTH_FIELD, textLength);
        builder.field(LAST_MODIFIED_FIELD, lastModified.toString());
        builder.startArray(CHUNKS_FIELD);
        for (SemanticTextField.Chunk chunk : chunks) {
            builder.startObject();
            builder.field(START_OFFSET_FIELD, chunk.startOffset());
            builder.field(END_OFFSET_FIELD, chunk.endOffset());
            XContentParser embeddingParser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY,
                chunk.rawEmbeddings(),
                XContentType.JSON
            );
            builder.field(EMBEDDINGS_FIELD).copyCurrentStructure(embeddingParser);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SemanticTextField.Chunk, Void> CHUNK_PARSER =
        new ConstructingObjectParser<>("staged_chunk", true, args -> {
            int startOffset = (int) args[0];
            int endOffset = (int) args[1];
            BytesReference embeddings = (BytesReference) args[2];
            return new SemanticTextField.Chunk(startOffset, endOffset, embeddings);
        });

    static {
        CHUNK_PARSER.declareInt(constructorArg(), new ParseField(START_OFFSET_FIELD));
        CHUNK_PARSER.declareInt(constructorArg(), new ParseField(END_OFFSET_FIELD));
        CHUNK_PARSER.declareField(constructorArg(), (p, c) -> {
            try (XContentBuilder b = XContentBuilder.builder(p.contentType().xContent())) {
                b.copyCurrentStructure(p);
                return BytesReference.bytes(b);
            }
        }, new ParseField(EMBEDDINGS_FIELD), ObjectParser.ValueType.VALUE_OBJECT_ARRAY);
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<StagedSemanticField, Void> PARSER =
        new ConstructingObjectParser<>("staged_semantic_field", true, args -> {
            String text = (String) args[0];
            int textLength = (int) args[1];
            Instant lastModified = Instant.parse((String) args[2]);
            List<SemanticTextField.Chunk> chunks = (List<SemanticTextField.Chunk>) args[3];
            return new StagedSemanticField(text, textLength, lastModified, chunks != null ? chunks : List.of());
        });

    static {
        PARSER.declareString(constructorArg(), new ParseField(TEXT_FIELD));
        PARSER.declareInt(constructorArg(), new ParseField(TEXT_LENGTH_FIELD));
        PARSER.declareString(constructorArg(), new ParseField(LAST_MODIFIED_FIELD));
        PARSER.declareObjectArray(optionalConstructorArg(), CHUNK_PARSER, new ParseField(CHUNKS_FIELD));
    }

    public static StagedSemanticField fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
```

Note: The `ObjectParser.ValueType` import and the embedding parsing approach above is illustrative. During implementation, consult how `SemanticTextField` handles `rawEmbeddings` parsing — specifically the `CHUNKS_PARSER` in `SemanticTextField.java` around line 380 — and follow the same pattern. The key is that embeddings are parsed as raw bytes and stored as `BytesReference` without interpreting them.

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.mapper.StagedSemanticFieldTests" -Dtests.iters=1`
Expected: All 3 tests PASS.

- [ ] **Step 5: Run spotless and commit**

```bash
./gradlew :x-pack:plugin:inference:spotlessApply
git add x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/StagedSemanticField.java \
        x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/mapper/StagedSemanticFieldTests.java
git commit -m "Add StagedSemanticField record for multi-part BYO staging"
```

---

## Task 3: BYOSemanticValidator — Per-Chunk Validation

**Files:**
- Create: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticValidator.java`
- Create: `x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticValidatorTests.java`

- [ ] **Step 1: Write failing tests for per-chunk validation**

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class BYOSemanticValidatorTests extends ESTestCase {

    private static final BytesReference DUMMY_EMBEDDING = new BytesArray("{\"values\":[0.1,0.2,0.3]}");

    // --- Per-chunk validation ---

    public void testValidChunk() {
        // Should not throw
        BYOSemanticValidator.validateChunk(0, 100, DUMMY_EMBEDDING, 200);
    }

    public void testNegativeStartOffset() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BYOSemanticValidator.validateChunk(-1, 100, DUMMY_EMBEDDING, 200)
        );
        assertThat(e.getMessage(), containsString("start_offset [-1] must be non-negative"));
    }

    public void testNegativeEndOffset() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BYOSemanticValidator.validateChunk(0, -5, DUMMY_EMBEDDING, 200)
        );
        assertThat(e.getMessage(), containsString("end_offset [-5] must be positive"));
    }

    public void testStartOffsetEqualsEndOffset() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BYOSemanticValidator.validateChunk(50, 50, DUMMY_EMBEDDING, 200)
        );
        assertThat(e.getMessage(), containsString("start_offset [50] must be less than end_offset [50]"));
    }

    public void testStartOffsetGreaterThanEndOffset() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BYOSemanticValidator.validateChunk(100, 50, DUMMY_EMBEDDING, 200)
        );
        assertThat(e.getMessage(), containsString("start_offset [100] must be less than end_offset [50]"));
    }

    public void testEndOffsetExceedsTextLength() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BYOSemanticValidator.validateChunk(0, 201, DUMMY_EMBEDDING, 200)
        );
        assertThat(e.getMessage(), containsString("end_offset [201] exceeds text_length [200]"));
    }

    public void testEndOffsetEqualsTextLengthIsValid() {
        // Boundary: end_offset == text_length should be valid
        BYOSemanticValidator.validateChunk(0, 200, DUMMY_EMBEDDING, 200);
    }

    public void testNullEmbeddings() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BYOSemanticValidator.validateChunk(0, 100, null, 200)
        );
        assertThat(e.getMessage(), containsString("embeddings are required"));
    }

    public void testEmptyEmbeddings() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BYOSemanticValidator.validateChunk(0, 100, BytesReference.fromByteBuffer(java.nio.ByteBuffer.allocate(0)), 200)
        );
        assertThat(e.getMessage(), containsString("embeddings are required"));
    }

    public void testChunkSpanningEntireText() {
        // Single chunk covering full text should be valid
        BYOSemanticValidator.validateChunk(0, 500, DUMMY_EMBEDDING, 500);
    }

    public void testChunkAtEndOfText() {
        BYOSemanticValidator.validateChunk(490, 500, DUMMY_EMBEDDING, 500);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.mapper.BYOSemanticValidatorTests" -Dtests.iters=1`
Expected: Compilation failure — `BYOSemanticValidator` does not exist.

- [ ] **Step 3: Implement validateChunk**

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.common.bytes.BytesReference;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Validates BYO chunk and embedding data for semantic_text fields.
 * Provides per-chunk validation (bounds, offset integrity), cross-chunk overlap detection,
 * and commit-time full coverage validation.
 */
public final class BYOSemanticValidator {

    private BYOSemanticValidator() {}

    /**
     * Validates a single chunk's offsets and embeddings against the known text length.
     *
     * @param startOffset  start character offset in original text
     * @param endOffset    end character offset (exclusive) in original text
     * @param embeddings   raw embedding bytes; must be non-null and non-empty
     * @param textLength   total character length of the original text
     * @throws IllegalArgumentException if any validation fails
     */
    public static void validateChunk(int startOffset, int endOffset, BytesReference embeddings, int textLength) {
        if (startOffset < 0) {
            throw new IllegalArgumentException("start_offset [" + startOffset + "] must be non-negative");
        }
        if (endOffset <= 0) {
            throw new IllegalArgumentException("end_offset [" + endOffset + "] must be positive");
        }
        if (startOffset >= endOffset) {
            throw new IllegalArgumentException(
                "start_offset [" + startOffset + "] must be less than end_offset [" + endOffset + "]"
            );
        }
        if (endOffset > textLength) {
            throw new IllegalArgumentException(
                "end_offset [" + endOffset + "] exceeds text_length [" + textLength + "]"
            );
        }
        if (embeddings == null || embeddings.length() == 0) {
            throw new IllegalArgumentException("embeddings are required and must be non-empty");
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.mapper.BYOSemanticValidatorTests" -Dtests.iters=1`
Expected: All tests PASS.

- [ ] **Step 5: Run spotless and commit**

```bash
./gradlew :x-pack:plugin:inference:spotlessApply
git add x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticValidator.java \
        x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticValidatorTests.java
git commit -m "Add BYOSemanticValidator with per-chunk validation"
```

---

## Task 4: BYOSemanticValidator — Cross-Chunk Overlap Detection

**Files:**
- Modify: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticValidator.java`
- Modify: `x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticValidatorTests.java`

- [ ] **Step 1: Add failing tests for overlap detection**

Append to `BYOSemanticValidatorTests.java`:

```java
    // --- Cross-chunk overlap detection ---

    public void testNoOverlapAdjacentChunks() {
        List<SemanticTextField.Chunk> existing = List.of(
            new SemanticTextField.Chunk(0, 100, DUMMY_EMBEDDING)
        );
        List<SemanticTextField.Chunk> incoming = List.of(
            new SemanticTextField.Chunk(100, 200, DUMMY_EMBEDDING)
        );
        // Should not throw
        BYOSemanticValidator.validateNoOverlaps(existing, incoming);
    }

    public void testOverlapWithExistingChunk() {
        List<SemanticTextField.Chunk> existing = List.of(
            new SemanticTextField.Chunk(0, 100, DUMMY_EMBEDDING)
        );
        List<SemanticTextField.Chunk> incoming = List.of(
            new SemanticTextField.Chunk(50, 150, DUMMY_EMBEDDING)
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BYOSemanticValidator.validateNoOverlaps(existing, incoming)
        );
        assertThat(e.getMessage(), containsString("overlaps"));
    }

    public void testOverlapWithinIncomingBatch() {
        List<SemanticTextField.Chunk> existing = List.of();
        List<SemanticTextField.Chunk> incoming = List.of(
            new SemanticTextField.Chunk(0, 100, DUMMY_EMBEDDING),
            new SemanticTextField.Chunk(50, 150, DUMMY_EMBEDDING)
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BYOSemanticValidator.validateNoOverlaps(existing, incoming)
        );
        assertThat(e.getMessage(), containsString("overlaps"));
    }

    public void testDuplicateRangeRejected() {
        List<SemanticTextField.Chunk> existing = List.of(
            new SemanticTextField.Chunk(0, 100, DUMMY_EMBEDDING)
        );
        List<SemanticTextField.Chunk> incoming = List.of(
            new SemanticTextField.Chunk(0, 100, DUMMY_EMBEDDING)
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BYOSemanticValidator.validateNoOverlaps(existing, incoming)
        );
        assertThat(e.getMessage(), containsString("overlaps"));
    }

    public void testNonContiguousNonOverlappingChunksAllowed() {
        // Chunks with gaps between them are fine at stage time (gaps checked at commit)
        List<SemanticTextField.Chunk> existing = List.of(
            new SemanticTextField.Chunk(0, 50, DUMMY_EMBEDDING)
        );
        List<SemanticTextField.Chunk> incoming = List.of(
            new SemanticTextField.Chunk(100, 200, DUMMY_EMBEDDING)
        );
        BYOSemanticValidator.validateNoOverlaps(existing, incoming);
    }

    public void testOutOfOrderChunksNoOverlap() {
        // Chunks arriving out of order should be handled correctly
        List<SemanticTextField.Chunk> existing = List.of(
            new SemanticTextField.Chunk(200, 300, DUMMY_EMBEDDING)
        );
        List<SemanticTextField.Chunk> incoming = List.of(
            new SemanticTextField.Chunk(0, 100, DUMMY_EMBEDDING),
            new SemanticTextField.Chunk(100, 200, DUMMY_EMBEDDING)
        );
        BYOSemanticValidator.validateNoOverlaps(existing, incoming);
    }

    public void testManyChunksPartialOverlap() {
        List<SemanticTextField.Chunk> existing = List.of(
            new SemanticTextField.Chunk(0, 100, DUMMY_EMBEDDING),
            new SemanticTextField.Chunk(100, 200, DUMMY_EMBEDDING),
            new SemanticTextField.Chunk(200, 300, DUMMY_EMBEDDING)
        );
        List<SemanticTextField.Chunk> incoming = List.of(
            new SemanticTextField.Chunk(299, 400, DUMMY_EMBEDDING)
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BYOSemanticValidator.validateNoOverlaps(existing, incoming)
        );
        assertThat(e.getMessage(), containsString("overlaps"));
    }

    public void testSameChunkTwiceInIncomingBatch() {
        List<SemanticTextField.Chunk> existing = List.of();
        List<SemanticTextField.Chunk> incoming = List.of(
            new SemanticTextField.Chunk(0, 100, DUMMY_EMBEDDING),
            new SemanticTextField.Chunk(0, 100, DUMMY_EMBEDDING)
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BYOSemanticValidator.validateNoOverlaps(existing, incoming)
        );
        assertThat(e.getMessage(), containsString("overlaps"));
    }
```

- [ ] **Step 2: Run tests to verify new tests fail**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.mapper.BYOSemanticValidatorTests" -Dtests.iters=1`
Expected: Compilation failure — `validateNoOverlaps` does not exist.

- [ ] **Step 3: Implement validateNoOverlaps**

Add to `BYOSemanticValidator.java`:

```java
    /**
     * Validates that incoming chunks do not overlap with each other or with existing staged chunks.
     * Chunks are sorted by start_offset and checked pairwise for interval intersection.
     *
     * @param existingChunks already-staged chunks
     * @param incomingChunks new chunks being added in this request
     * @throws IllegalArgumentException if any overlap is detected
     */
    public static void validateNoOverlaps(
        List<SemanticTextField.Chunk> existingChunks,
        List<SemanticTextField.Chunk> incomingChunks
    ) {
        List<SemanticTextField.Chunk> all = new ArrayList<>(existingChunks.size() + incomingChunks.size());
        all.addAll(existingChunks);
        all.addAll(incomingChunks);
        all.sort(Comparator.comparingInt(SemanticTextField.Chunk::startOffset)
            .thenComparingInt(SemanticTextField.Chunk::endOffset));

        for (int i = 1; i < all.size(); i++) {
            SemanticTextField.Chunk prev = all.get(i - 1);
            SemanticTextField.Chunk curr = all.get(i);
            if (curr.startOffset() < prev.endOffset()) {
                throw new IllegalArgumentException(
                    "Chunk [" + curr.startOffset() + ", " + curr.endOffset() + ") overlaps with chunk ["
                        + prev.startOffset() + ", " + prev.endOffset() + ")"
                );
            }
        }
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.mapper.BYOSemanticValidatorTests" -Dtests.iters=1`
Expected: All tests PASS.

- [ ] **Step 5: Run spotless and commit**

```bash
./gradlew :x-pack:plugin:inference:spotlessApply
git add x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticValidator.java \
        x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticValidatorTests.java
git commit -m "Add cross-chunk overlap detection to BYOSemanticValidator"
```

---

## Task 5: BYOSemanticValidator — Commit-Time Coverage and Dimensional Validation

**Files:**
- Modify: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticValidator.java`
- Modify: `x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticValidatorTests.java`

- [ ] **Step 1: Write failing tests for commit-time coverage validation**

Append to `BYOSemanticValidatorTests.java`:

```java
    // --- Commit-time coverage validation ---

    public void testFullCoverage() {
        List<SemanticTextField.Chunk> chunks = List.of(
            new SemanticTextField.Chunk(0, 100, DUMMY_EMBEDDING),
            new SemanticTextField.Chunk(100, 200, DUMMY_EMBEDDING),
            new SemanticTextField.Chunk(200, 300, DUMMY_EMBEDDING)
        );
        // Should not throw
        BYOSemanticValidator.validateFullCoverage(chunks, 300);
    }

    public void testFullCoverageOutOfOrder() {
        // Chunks arriving out of order should still pass if they cover the full range
        List<SemanticTextField.Chunk> chunks = List.of(
            new SemanticTextField.Chunk(200, 300, DUMMY_EMBEDDING),
            new SemanticTextField.Chunk(0, 100, DUMMY_EMBEDDING),
            new SemanticTextField.Chunk(100, 200, DUMMY_EMBEDDING)
        );
        BYOSemanticValidator.validateFullCoverage(chunks, 300);
    }

    public void testGapAtStart() {
        List<SemanticTextField.Chunk> chunks = List.of(
            new SemanticTextField.Chunk(10, 100, DUMMY_EMBEDDING)
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BYOSemanticValidator.validateFullCoverage(chunks, 100)
        );
        assertThat(e.getMessage(), containsString("Gap detected"));
        assertThat(e.getMessage(), containsString("[0, 10)"));
    }

    public void testGapInMiddle() {
        List<SemanticTextField.Chunk> chunks = List.of(
            new SemanticTextField.Chunk(0, 100, DUMMY_EMBEDDING),
            new SemanticTextField.Chunk(150, 300, DUMMY_EMBEDDING)
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BYOSemanticValidator.validateFullCoverage(chunks, 300)
        );
        assertThat(e.getMessage(), containsString("Gap detected"));
        assertThat(e.getMessage(), containsString("[100, 150)"));
    }

    public void testGapAtEnd() {
        List<SemanticTextField.Chunk> chunks = List.of(
            new SemanticTextField.Chunk(0, 100, DUMMY_EMBEDDING)
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BYOSemanticValidator.validateFullCoverage(chunks, 200)
        );
        assertThat(e.getMessage(), containsString("Gap detected"));
        assertThat(e.getMessage(), containsString("[100, 200)"));
    }

    public void testEmptyChunksListFails() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BYOSemanticValidator.validateFullCoverage(List.of(), 100)
        );
        assertThat(e.getMessage(), containsString("No chunks"));
    }

    public void testSingleChunkFullCoverage() {
        List<SemanticTextField.Chunk> chunks = List.of(
            new SemanticTextField.Chunk(0, 500, DUMMY_EMBEDDING)
        );
        BYOSemanticValidator.validateFullCoverage(chunks, 500);
    }

    // --- Maximum text length edge case ---

    public void testMaximumSizeTextFieldCoverage() {
        // Simulate a very large text field with many chunks
        int textLength = 1_000_000; // 1M characters
        int chunkSize = 512;
        List<SemanticTextField.Chunk> chunks = new ArrayList<>();
        for (int offset = 0; offset < textLength; offset += chunkSize) {
            int end = Math.min(offset + chunkSize, textLength);
            chunks.add(new SemanticTextField.Chunk(offset, end, DUMMY_EMBEDDING));
        }
        // Should not throw, even with ~1953 chunks
        BYOSemanticValidator.validateFullCoverage(chunks, textLength);
    }

    // --- Dimensional validation ---

    public void testValidDimensions() {
        BytesReference embedding768 = new BytesArray("[0.1,0.2,0.3]"); // simplified
        // For actual implementation, this will parse the embedding and count elements.
        // This test verifies the validation method signature and basic behavior.
        BYOSemanticValidator.validateEmbeddingDimensions(embedding768, 3, "test_field", 0, 100);
    }

    public void testDimensionMismatch() {
        BytesReference embedding3 = new BytesArray("[0.1,0.2,0.3]");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BYOSemanticValidator.validateEmbeddingDimensions(embedding3, 768, "test_field", 0, 100)
        );
        assertThat(e.getMessage(), containsString("Expected 768 dimensions"));
    }
```

- [ ] **Step 2: Run tests to verify new tests fail**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.mapper.BYOSemanticValidatorTests" -Dtests.iters=1`
Expected: Compilation failure — `validateFullCoverage` and `validateEmbeddingDimensions` do not exist.

- [ ] **Step 3: Implement validateFullCoverage and validateEmbeddingDimensions**

Add to `BYOSemanticValidator.java`:

```java
    /**
     * Validates that the chunks cover the full text range [0, textLength) with no gaps.
     * Chunks do not need to arrive in order — they are sorted by start_offset before checking.
     *
     * @param chunks     all chunks (existing + incoming)
     * @param textLength total character length of the text
     * @throws IllegalArgumentException if any gap is found or chunks are empty
     */
    public static void validateFullCoverage(List<SemanticTextField.Chunk> chunks, int textLength) {
        if (chunks.isEmpty()) {
            throw new IllegalArgumentException("No chunks provided. At least one chunk is required to cover text_length [" + textLength + "]");
        }

        List<SemanticTextField.Chunk> sorted = new ArrayList<>(chunks);
        sorted.sort(Comparator.comparingInt(SemanticTextField.Chunk::startOffset));

        // Check first chunk starts at 0
        if (sorted.get(0).startOffset() != 0) {
            throw new IllegalArgumentException(
                "Gap detected between offsets [0, " + sorted.get(0).startOffset() + "). "
                    + "First chunk must start at offset 0"
            );
        }

        // Check contiguous coverage
        for (int i = 1; i < sorted.size(); i++) {
            SemanticTextField.Chunk prev = sorted.get(i - 1);
            SemanticTextField.Chunk curr = sorted.get(i);
            if (curr.startOffset() != prev.endOffset()) {
                throw new IllegalArgumentException(
                    "Gap detected between offsets [" + prev.endOffset() + ", " + curr.startOffset() + ")"
                );
            }
        }

        // Check last chunk ends at textLength
        SemanticTextField.Chunk last = sorted.get(sorted.size() - 1);
        if (last.endOffset() != textLength) {
            throw new IllegalArgumentException(
                "Gap detected between offsets [" + last.endOffset() + ", " + textLength + "). "
                    + "Chunks must cover the full text_length [" + textLength + "]"
            );
        }
    }

    /**
     * Validates that the embedding at the given chunk has the expected number of dimensions.
     * Parses the embedding bytes as a JSON array and counts elements.
     *
     * @param embeddings       raw embedding bytes
     * @param expectedDims     expected number of dimensions from model_settings
     * @param fieldName        field name for error messages
     * @param startOffset      chunk start offset for error messages
     * @param endOffset        chunk end offset for error messages
     * @throws IllegalArgumentException if dimensions do not match
     */
    public static void validateEmbeddingDimensions(
        BytesReference embeddings,
        int expectedDims,
        String fieldName,
        int startOffset,
        int endOffset
    ) {
        // Parse the embeddings to count dimensions
        int actualDims = countEmbeddingDimensions(embeddings);
        if (actualDims != expectedDims) {
            throw new IllegalArgumentException(
                "Expected " + expectedDims + " dimensions for field [" + fieldName
                    + "], chunk at [" + startOffset + ", " + endOffset + ") has " + actualDims
            );
        }
    }

    private static int countEmbeddingDimensions(BytesReference embeddings) {
        try (XContentParser parser = XContentHelper.createParserNotCompressed(
            XContentParserConfiguration.EMPTY,
            embeddings,
            XContentType.JSON
        )) {
            if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                throw new IllegalArgumentException("Embeddings must be a JSON array");
            }
            int count = 0;
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                count++;
            }
            return count;
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse embeddings: " + e.getMessage(), e);
        }
    }
```

Note: Add the necessary imports (`XContentHelper`, `XContentParser`, `XContentParserConfiguration`, `XContentType`, `IOException`) at the top of the file. Consult the existing import patterns in `SemanticTextField.java`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.mapper.BYOSemanticValidatorTests" -Dtests.iters=1`
Expected: All tests PASS.

- [ ] **Step 5: Run spotless and commit**

```bash
./gradlew :x-pack:plugin:inference:spotlessApply
git add x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticValidator.java \
        x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticValidatorTests.java
git commit -m "Add commit-time coverage and dimensional validation to BYOSemanticValidator"
```

---

## Task 6: BYOSemanticHandler — Detection and Dispatch

**Files:**
- Create: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticHandler.java`
- Modify: `x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticHandlerTests.java`

- [ ] **Step 1: Write failing tests for BYO field value detection**

Append to `BYOSemanticHandlerTests.java`:

```java
    // --- Field value detection ---

    public void testDetectStringValueIsNotBYO() {
        assertFalse(BYOSemanticHandler.isBYOValue("some text string"));
    }

    public void testDetectNullValueIsNotBYO() {
        assertFalse(BYOSemanticHandler.isBYOValue(null));
    }

    public void testDetectMapWithActionIsBYO() {
        Map<String, Object> value = Map.of("_action", "stage_init", "text", "hello");
        assertTrue(BYOSemanticHandler.isBYOValue(value));
    }

    public void testDetectMapWithTextAndChunksIsBYO() {
        Map<String, Object> value = Map.of(
            "text", "hello",
            "chunks", List.of(Map.of("start_offset", 0, "end_offset", 5, "embeddings", List.of(0.1)))
        );
        assertTrue(BYOSemanticHandler.isBYOValue(value));
    }

    public void testDetectMapWithoutActionOrChunksIsNotBYO() {
        // A Map that looks like an already-computed inference result (existing bypass)
        // should NOT be treated as BYO
        Map<String, Object> value = Map.of("inference", Map.of("inference_id", "my-endpoint"));
        assertFalse(BYOSemanticHandler.isBYOValue(value));
    }

    public void testDetectActionType() {
        Map<String, Object> stageInit = Map.of("_action", "stage_init", "text", "hello");
        assertThat(BYOSemanticHandler.getAction(stageInit), equalTo(BYOSemanticAction.STAGE_INIT));

        Map<String, Object> stage = Map.of("_action", "stage", "chunks", List.of());
        assertThat(BYOSemanticHandler.getAction(stage), equalTo(BYOSemanticAction.STAGE));

        Map<String, Object> commit = Map.of("_action", "commit");
        assertThat(BYOSemanticHandler.getAction(commit), equalTo(BYOSemanticAction.COMMIT));

        Map<String, Object> cancel = Map.of("_action", "cancel");
        assertThat(BYOSemanticHandler.getAction(cancel), equalTo(BYOSemanticAction.CANCEL));
    }

    public void testSingleShotBYOHasNoAction() {
        Map<String, Object> value = Map.of(
            "text", "hello",
            "chunks", List.of()
        );
        assertNull(BYOSemanticHandler.getAction(value));
    }
```

- [ ] **Step 2: Run tests to verify new tests fail**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.mapper.BYOSemanticHandlerTests" -Dtests.iters=1`
Expected: Compilation failure — `BYOSemanticHandler` does not exist.

- [ ] **Step 3: Implement BYOSemanticHandler detection methods**

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.core.Nullable;

import java.util.Map;

/**
 * Detects and dispatches BYO (bring-your-own) semantic_text field values.
 * A BYO value is a Map (not a String) in a semantic_text field that contains either:
 * <ul>
 *   <li>An {@code _action} key (multi-part staging protocol), or</li>
 *   <li>Both {@code text} and {@code chunks} keys (single-shot BYO)</li>
 * </ul>
 */
public final class BYOSemanticHandler {

    private BYOSemanticHandler() {}

    /**
     * Returns true if the field value represents a BYO semantic_text input
     * (as opposed to a plain string for inference or a pre-computed inference result Map).
     */
    public static boolean isBYOValue(@Nullable Object fieldValue) {
        if (fieldValue instanceof Map<?, ?> map) {
            return map.containsKey(BYOSemanticAction.actionField())
                || (map.containsKey("text") && map.containsKey("chunks"));
        }
        return false;
    }

    /**
     * Extracts the BYO action from the field value Map, or null for single-shot BYO
     * (which has text+chunks but no _action).
     */
    @Nullable
    public static BYOSemanticAction getAction(Map<String, Object> fieldValue) {
        Object action = fieldValue.get(BYOSemanticAction.actionField());
        if (action == null) {
            return null;
        }
        return BYOSemanticAction.fromString(action.toString());
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.mapper.BYOSemanticHandlerTests" -Dtests.iters=1`
Expected: All tests PASS.

- [ ] **Step 5: Run spotless and commit**

```bash
./gradlew :x-pack:plugin:inference:spotlessApply
git add x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticHandler.java \
        x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticHandlerTests.java
git commit -m "Add BYOSemanticHandler with field value detection"
```

---

## Task 7: BYOSemanticHandler — stage_init, stage, commit, cancel Logic

**Files:**
- Modify: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticHandler.java`
- Create: `x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/action/filter/ShardBulkInferenceActionFilterBYOTests.java`

This is the largest task. It implements the core staging logic and wires it into the filter's document source manipulation.

- [ ] **Step 1: Write failing tests for the full BYO flow through the filter**

Create `ShardBulkInferenceActionFilterBYOTests.java`. This test class follows the same structure as `ShardBulkInferenceActionFilterTests` (extending `ESTestCase`, parameterized on `useLegacyFormat`, using `TestThreadPool`). Below are the test methods — each tests a specific scenario from the spec.

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.telemetry.InferenceStats;
import org.elasticsearch.inference.telemetry.InferenceStatsTests;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;
import org.elasticsearch.xpack.inference.model.TestModel;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.awaitLatch;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

/**
 * Tests BYO (bring-your-own) chunks and vectors flow through ShardBulkInferenceActionFilter.
 * These tests verify the full lifecycle: stage_init, stage, commit, cancel, single-shot BYO,
 * and all error conditions.
 */
public class ShardBulkInferenceActionFilterBYOTests extends ESTestCase {

    // NOTE TO IMPLEMENTER: Use the same createFilter() helper pattern from
    // ShardBulkInferenceActionFilterTests. Either extract it to a shared test utility
    // or duplicate the relevant setup. The StaticModel should be configured with
    // TEXT_EMBEDDING task type, 3 dimensions, COSINE similarity to match the test embeddings.

    private ThreadPool threadPool;

    @Before
    public void setupThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() throws Exception {
        terminate(threadPool);
    }

    // === SINGLE-SHOT BYO TESTS ===

    public void testSingleShotBYOValidDocument() throws Exception {
        // Send a complete BYO document with text + all chunks in one request.
        // Verify: inference is NOT called, document is indexed with chunks in _inference_metadata,
        // model_settings are resolved from the inference endpoint.
    }

    public void testSingleShotBYOMissingText() throws Exception {
        // Send BYO object with chunks but no "text" key.
        // Verify: request fails with error about missing text.
    }

    public void testSingleShotBYOEmptyText() throws Exception {
        // Send BYO object with text="" and chunks.
        // Verify: request fails with error about empty text.
    }

    public void testSingleShotBYOGapInChunks() throws Exception {
        // Send BYO object where chunks have a gap (e.g., [0,100) and [150,200) for text of length 200).
        // Verify: request fails with gap error.
    }

    public void testSingleShotBYOOverlappingChunks() throws Exception {
        // Send BYO object where two chunks overlap (e.g., [0,100) and [50,150)).
        // Verify: request fails with overlap error.
    }

    public void testSingleShotBYODimensionMismatch() throws Exception {
        // Send BYO object where embeddings have wrong number of dimensions
        // (model expects 3 but embeddings have 5).
        // Verify: request fails with dimension mismatch error.
    }

    public void testSingleShotBYOEndOffsetExceedsTextLength() throws Exception {
        // Send BYO object where a chunk's end_offset > text length.
        // Verify: request fails with bounds error.
    }

    // === MULTI-PART STAGING TESTS ===

    public void testStageInitCreatesStaged() throws Exception {
        // Send stage_init with text and first batch of chunks.
        // Verify: _staged section is created in _inference_metadata with correct text,
        // text_length, last_modified, and chunks.
        // Verify: document is NOT searchable (no committed chunks).
    }

    public void testStageInitTextOnly() throws Exception {
        // Send stage_init with text but no chunks.
        // Verify: _staged is created with empty chunks list.
    }

    public void testStageAppendsChunks() throws Exception {
        // First: stage_init with text + chunk [0,100).
        // Then: stage with chunk [100,200).
        // Verify: _staged now contains both chunks.
        // Verify: last_modified is updated.
    }

    public void testStageOutOfOrderChunks() throws Exception {
        // First: stage_init with text (length 300) + chunk [200,300).
        // Then: stage with chunk [0,100).
        // Then: stage with chunk [100,200).
        // Verify: all three chunks are in _staged.
        // Verify: no error even though chunks arrived out of order.
    }

    public void testCommitPromotesChunks() throws Exception {
        // Full lifecycle: stage_init + stage + commit.
        // Verify: _staged is removed, chunks are in committed _inference_metadata,
        // model_settings are populated, document IS searchable.
    }

    public void testCommitWithFinalChunks() throws Exception {
        // stage_init with text + chunk [0,100), then commit with chunk [100,200).
        // Verify: commit includes the final chunk, validates, and promotes all.
    }

    public void testCancelClearsStaged() throws Exception {
        // stage_init + stage, then cancel.
        // Verify: _staged is removed, document has no committed chunks.
    }

    // === ERROR CONDITION TESTS ===

    public void testStageWithoutInitFails() throws Exception {
        // Send a stage action on a document that has no _staged.
        // Verify: error says "must stage_init first".
    }

    public void testCommitWithoutInitFails() throws Exception {
        // Send commit on a document with no _staged.
        // Verify: error says "no staged data".
    }

    public void testCancelWithoutInitFails() throws Exception {
        // Send cancel on a document with no _staged.
        // Verify: error says "no staged data" (or succeeds as no-op — design choice).
    }

    public void testDoubleStageInitFails() throws Exception {
        // stage_init, then stage_init again without commit/cancel.
        // Verify: second stage_init is rejected.
    }

    public void testStageOverlappingChunksAcrossBatches() throws Exception {
        // stage_init with chunk [0,100), then stage with chunk [50,150).
        // Verify: second stage fails with overlap error.
        // Verify: original _staged is preserved with only chunk [0,100).
    }

    public void testSameChunkTwiceInDifferentBatches() throws Exception {
        // stage_init with chunk [0,100), then stage with exact same chunk [0,100).
        // Verify: rejected as duplicate/overlap.
    }

    public void testCommitWithIncompleteConverageFails() throws Exception {
        // stage_init with text (length 300) + chunks [0,100) and [200,300).
        // Then commit (without sending chunk [100,200)).
        // Verify: commit fails with gap error [100,200).
        // Verify: _staged is preserved (user can retry).
    }

    public void testStageAfterCommitFails() throws Exception {
        // Full successful commit, then send another stage request.
        // Verify: stage fails (no _staged exists — it was cleared on commit).
    }

    // === INTERACTION WITH NORMAL UPDATES ===

    public void testNormalUpdatePreservesStaged() throws Exception {
        // stage_init on semantic field, then _update a different field on the same doc.
        // Verify: _staged data is still present after the update.
    }

    public void testStringValueOverwritesClearsStaged() throws Exception {
        // stage_init on semantic field, then index the same field with a plain string.
        // Verify: _staged is cleared, inference runs on the string, document has
        // inference-generated chunks (not the BYO staged ones).
    }

    // === BACKWARD COMPATIBILITY ===

    public void testStringValueStillTriggersInference() throws Exception {
        // Index a document with a plain string in the semantic_text field.
        // Verify: inference runs as normal, no BYO handling is triggered.
    }

    // === EDGE CASES ===

    public void testMaximumSizeTextField() throws Exception {
        // Create a text field at the maximum allowed size (near http.max_content_length).
        // stage_init with the text + first batch of chunks covering [0, chunkSize).
        // stage with remaining chunks in batches.
        // commit.
        // Verify: all chunks are promoted correctly.
        // This tests that multi-part upload handles very large documents.
        // Use at least 10 chunks to simulate realistic batching.
    }

    public void testSingleChunkCoveringEntireText() throws Exception {
        // BYO with just one chunk covering [0, text_length).
        // Verify: single-shot BYO works with a single chunk.
    }

    public void testStageInitWithAllChunksThenCommit() throws Exception {
        // stage_init with text + ALL chunks (nothing left to stage).
        // Then just commit (no additional chunks).
        // Verify: commit succeeds — this is the "I could have used single-shot but chose staging" case.
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilterBYOTests" -Dtests.iters=1`
Expected: Compilation failure — test methods reference unimplemented infrastructure.

- [ ] **Step 3: Implement BYOSemanticHandler staging logic**

Add the following methods to `BYOSemanticHandler.java`. These handle the core state transitions. The handler reads/modifies the document source map (specifically `_inference_metadata.<field>._staged`) and delegates validation to `BYOSemanticValidator`.

```java
    /**
     * Handles a stage_init action. Creates the _staged section with the provided text
     * and optional first batch of chunks.
     *
     * @param fieldName     the semantic_text field name
     * @param fieldValue    the parsed field value Map containing text, optional chunks
     * @param docSourceMap  the full document source map (mutable)
     * @param now           current timestamp for last_modified
     * @throws IllegalArgumentException if _staged already exists or text is missing
     */
    public static void handleStageInit(
        String fieldName,
        Map<String, Object> fieldValue,
        Map<String, Object> docSourceMap,
        java.time.Instant now
    ) {
        // 1. Check no existing _staged
        Map<String, Object> existingStaged = getStaged(fieldName, docSourceMap);
        if (existingStaged != null) {
            throw new IllegalArgumentException(
                "Field [" + fieldName + "] already has staged data. Commit or cancel before re-initializing."
            );
        }

        // 2. Extract and validate text
        Object textObj = fieldValue.get("text");
        if (textObj == null || textObj.toString().isEmpty()) {
            throw new IllegalArgumentException("text is required for stage_init on field [" + fieldName + "]");
        }
        String text = textObj.toString();
        int textLength = text.length();

        // 3. Parse and validate optional chunks
        List<SemanticTextField.Chunk> chunks = parseChunksFromValue(fieldValue, textLength);

        // 4. Create StagedSemanticField and store in _inference_metadata
        StagedSemanticField staged = new StagedSemanticField(text, textLength, now, chunks);
        setStaged(fieldName, staged, docSourceMap);
    }

    /**
     * Handles a stage action. Appends chunks to an existing _staged section.
     */
    public static void handleStage(
        String fieldName,
        Map<String, Object> fieldValue,
        Map<String, Object> docSourceMap,
        java.time.Instant now
    ) {
        StagedSemanticField existing = getStagedOrThrow(fieldName, docSourceMap);

        List<SemanticTextField.Chunk> newChunks = parseChunksFromValue(fieldValue, existing.textLength());

        // Validate no overlaps with existing chunks
        BYOSemanticValidator.validateNoOverlaps(existing.chunks(), newChunks);

        // Append and update
        StagedSemanticField updated = existing.withAppendedChunks(newChunks, now);
        setStaged(fieldName, updated, docSourceMap);
    }

    /**
     * Handles a commit action. Validates full coverage, resolves model_settings,
     * promotes chunks to committed _inference_metadata, removes _staged.
     */
    public static void handleCommit(
        String fieldName,
        Map<String, Object> fieldValue,
        Map<String, Object> docSourceMap,
        String inferenceId,
        MinimalServiceSettings modelSettings,
        java.time.Instant now
    ) {
        StagedSemanticField existing = getStagedOrThrow(fieldName, docSourceMap);

        // Append optional final chunks
        List<SemanticTextField.Chunk> finalChunks = parseChunksFromValue(fieldValue, existing.textLength());
        if (finalChunks.isEmpty() == false) {
            BYOSemanticValidator.validateNoOverlaps(existing.chunks(), finalChunks);
            existing = existing.withAppendedChunks(finalChunks, now);
        }

        // Validate full coverage
        BYOSemanticValidator.validateFullCoverage(existing.chunks(), existing.textLength());

        // Validate dimensions if model_settings provides them
        if (modelSettings != null && modelSettings.dimensions() != null) {
            for (SemanticTextField.Chunk chunk : existing.chunks()) {
                BYOSemanticValidator.validateEmbeddingDimensions(
                    chunk.rawEmbeddings(),
                    modelSettings.dimensions(),
                    fieldName,
                    chunk.startOffset(),
                    chunk.endOffset()
                );
            }
        }

        // Promote: create committed SemanticTextField and remove _staged
        promoteToCommitted(fieldName, existing, inferenceId, modelSettings, docSourceMap);
    }

    /**
     * Handles a cancel action. Removes _staged section.
     */
    public static void handleCancel(String fieldName, Map<String, Object> docSourceMap) {
        getStagedOrThrow(fieldName, docSourceMap);
        removeStaged(fieldName, docSourceMap);
    }

    /**
     * Handles single-shot BYO (no _action). Validates everything atomically
     * and stores directly as committed inference data.
     */
    public static void handleSingleShot(
        String fieldName,
        Map<String, Object> fieldValue,
        Map<String, Object> docSourceMap,
        String inferenceId,
        MinimalServiceSettings modelSettings
    ) {
        Object textObj = fieldValue.get("text");
        if (textObj == null || textObj.toString().isEmpty()) {
            throw new IllegalArgumentException("text is required for BYO semantic field [" + fieldName + "]");
        }
        String text = textObj.toString();
        int textLength = text.length();

        List<SemanticTextField.Chunk> chunks = parseChunksFromValue(fieldValue, textLength);

        // Full atomic validation
        BYOSemanticValidator.validateNoOverlaps(List.of(), chunks);
        BYOSemanticValidator.validateFullCoverage(chunks, textLength);

        if (modelSettings != null && modelSettings.dimensions() != null) {
            for (SemanticTextField.Chunk chunk : chunks) {
                BYOSemanticValidator.validateEmbeddingDimensions(
                    chunk.rawEmbeddings(),
                    modelSettings.dimensions(),
                    fieldName,
                    chunk.startOffset(),
                    chunk.endOffset()
                );
            }
        }

        StagedSemanticField temp = new StagedSemanticField(text, textLength, java.time.Instant.now(), chunks);
        promoteToCommitted(fieldName, temp, inferenceId, modelSettings, docSourceMap);
    }
```

The private helper methods (`getStaged`, `getStagedOrThrow`, `setStaged`, `removeStaged`, `promoteToCommitted`, `parseChunksFromValue`) manipulate the `_inference_metadata` section of the document source map. Consult how `ShardBulkInferenceActionFilter.applyInferenceResponses()` (lines 897-989) builds and inserts the `SemanticTextField` into the document source. Follow the same pattern for the committed structure. For the `_staged` section, it should be a child of `_inference_metadata.<field>` alongside the committed `inference` section.

The `parseChunksFromValue` method extracts the `"chunks"` list from the field value Map, parsing each chunk's `start_offset`, `end_offset`, and `embeddings` fields. It calls `BYOSemanticValidator.validateChunk()` on each one. Consult `SemanticTextField.Chunk` constructors (lines 112-114 in `SemanticTextField.java`) for how to construct offset-form chunks with `BytesReference` embeddings.

- [ ] **Step 4: Fill in test method bodies**

Now implement each test method body in `ShardBulkInferenceActionFilterBYOTests.java`. Each test follows the same pattern used in the existing `ShardBulkInferenceActionFilterTests`:
1. Create a `StaticModel` with known dimensions (3)
2. Create a `ShardBulkInferenceActionFilter` via `createFilter()`
3. Build an `IndexRequest` with the BYO field value as a Map
4. Create `BulkShardRequest` with `InferenceFieldMetadata` mapping the field
5. Use `ActionFilterChain` + `CountDownLatch` to capture the processed request
6. Assert the expected outcome on the processed `BulkItemRequest`

For multi-step tests (stage_init → stage → commit), you must simulate multiple sequential bulk requests against the same document. Between requests, extract the updated source from the processed `IndexRequest` to use as the starting source for the next request. This simulates how Elasticsearch would persist and retrieve the document.

- [ ] **Step 5: Wire BYOSemanticHandler into ShardBulkInferenceActionFilter**

Modify `ShardBulkInferenceActionFilter.addFieldInferenceRequests()` (around line 625) to add BYO detection before the existing inference skip logic. The new code should be inserted at the top of the field iteration loop, before the legacy/non-legacy format bypass:

In `ShardBulkInferenceActionFilter.java`, find the method `addFieldInferenceRequests` and locate where it extracts the field value from the document. Add a check:

```java
// At the top of the per-field loop, before existing bypass logic:
var rawFieldValue = XContentMapValues.extractValue(field, docMap);
if (BYOSemanticHandler.isBYOValue(rawFieldValue)) {
    @SuppressWarnings("unchecked")
    Map<String, Object> byoValue = (Map<String, Object>) rawFieldValue;
    BYOSemanticAction action = BYOSemanticHandler.getAction(byoValue);
    // Handle BYO — delegate to BYOSemanticHandler methods
    // For stage_init, stage, cancel: operate directly on docMap
    // For commit and single-shot: need model_settings from modelRegistry
    // On success: continue (skip inference for this field)
    // On failure: set failure on the item and break
    continue;
}
```

The exact integration point requires careful reading of the surrounding code. The key constraint: BYO handling must happen before the inference request collection, and must either modify the document source in-place (for staging) or build the committed `SemanticTextField` structure (for commit/single-shot).

- [ ] **Step 6: Run all BYO tests to verify they pass**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilterBYOTests" -Dtests.iters=1`
Expected: All tests PASS.

- [ ] **Step 7: Run existing filter tests to verify no regression**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilterTests" -Dtests.iters=1`
Expected: All existing tests still PASS.

- [ ] **Step 8: Run spotless and commit**

```bash
./gradlew :x-pack:plugin:inference:spotlessApply
git add x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/BYOSemanticHandler.java \
        x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/action/filter/ShardBulkInferenceActionFilter.java \
        x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/action/filter/ShardBulkInferenceActionFilterBYOTests.java
git commit -m "Add BYOSemanticHandler with full staging lifecycle"
```

---

## Task 8: SemanticTextField — _staged Serialization Support

**Files:**
- Modify: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/SemanticTextField.java`
- Modify: `x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/mapper/SemanticTextFieldTests.java`

The committed `SemanticTextField` structure needs to support the `_staged` sibling when serializing/deserializing from `_inference_metadata`.

- [ ] **Step 1: Write failing tests for _staged in SemanticTextField serialization**

Add to `SemanticTextFieldTests.java`:

```java
    public void testRoundTripWithStagedSection() throws IOException {
        // This test is only relevant for non-legacy format
        if (useLegacyFormat) {
            return;
        }
        // Build a SemanticTextField that has a _staged sibling
        // Verify that toXContent includes _staged and fromXContent parses it back
        // The _staged section should be preserved through round-trip but is NOT
        // part of the committed SemanticTextField record — it's carried as a separate
        // field in _inference_metadata
    }
```

The exact integration depends on whether `_staged` is modeled as:
- A field on the `SemanticTextField` record itself (add `@Nullable StagedSemanticField staged` parameter), or
- A sibling in the `_inference_metadata` map handled by `SemanticInferenceMetadataFieldsMapper`

Recommendation: Keep `_staged` as a sibling key in the `_inference_metadata.<field>` map, handled by `BYOSemanticHandler`, not by `SemanticTextField` itself. This keeps the existing record clean. The `_staged` constant should be added to `SemanticTextField` for consistency:

```java
public static final String STAGED_FIELD = "_staged";
```

- [ ] **Step 2: Add the STAGED_FIELD constant and ensure parsing ignores it**

In `SemanticTextField.java`, add:
```java
public static final String STAGED_FIELD = "_staged";
```

Update the parser to ignore the `_staged` key if encountered (it should not fail on unknown fields since the parser uses `true` for `ignoreUnknownFields` — verify this is the case in the `ConstructingObjectParser` declaration at line 291).

- [ ] **Step 3: Run tests to verify they pass**

Run: `./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests" -Dtests.iters=1`
Expected: All tests PASS.

- [ ] **Step 4: Run spotless and commit**

```bash
./gradlew :x-pack:plugin:inference:spotlessApply
git add x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/mapper/SemanticTextField.java \
        x-pack/plugin/inference/src/test/java/org/elasticsearch/xpack/inference/mapper/SemanticTextFieldTests.java
git commit -m "Add STAGED_FIELD constant to SemanticTextField"
```

---

## Task 9: Index Setting — staged_ttl

**Files:**
- Modify: `server/src/main/java/org/elasticsearch/index/IndexSettings.java`
- Modify: `server/src/main/java/org/elasticsearch/common/settings/IndexScopedSettings.java`

- [ ] **Step 1: Add the setting definition**

In `IndexSettings.java`, add near the other time-based settings (around line 496):

```java
public static final TimeValue DEFAULT_SEMANTIC_TEXT_STAGED_TTL = TimeValue.timeValueHours(24);

public static final Setting<TimeValue> INDEX_SEMANTIC_TEXT_STAGED_TTL = Setting.timeSetting(
    "index.semantic_text.staged_ttl",
    DEFAULT_SEMANTIC_TEXT_STAGED_TTL,
    new TimeValue(-1, TimeUnit.MILLISECONDS),
    Property.Dynamic,
    Property.IndexScope
);
```

Add instance field and accessor:

```java
private volatile long semanticTextStagedTtlMillis;

public long getSemanticTextStagedTtlMillis() {
    return semanticTextStagedTtlMillis;
}
```

Initialize in constructor and register update consumer:

```java
semanticTextStagedTtlMillis = scopedSettings.get(INDEX_SEMANTIC_TEXT_STAGED_TTL).getMillis();
scopedSettings.addSettingsUpdateConsumer(INDEX_SEMANTIC_TEXT_STAGED_TTL, this::setSemanticTextStagedTtl);
```

Add setter:

```java
private void setSemanticTextStagedTtl(TimeValue ttl) {
    this.semanticTextStagedTtlMillis = ttl.getMillis();
}
```

- [ ] **Step 2: Register in IndexScopedSettings**

In `IndexScopedSettings.java`, add to `BUILT_IN_INDEX_SETTINGS`:

```java
IndexSettings.INDEX_SEMANTIC_TEXT_STAGED_TTL,
```

- [ ] **Step 3: Run server tests to verify no regression**

Run: `./gradlew :server:test --tests "org.elasticsearch.index.IndexSettingsTests" -Dtests.iters=1`
Expected: PASS (existing tests should not break from an additive setting).

- [ ] **Step 4: Run spotless and commit**

```bash
./gradlew :server:spotlessApply
git add server/src/main/java/org/elasticsearch/index/IndexSettings.java \
        server/src/main/java/org/elasticsearch/common/settings/IndexScopedSettings.java
git commit -m "Add index.semantic_text.staged_ttl setting"
```

---

## Task 10: REST Endpoint — _semantic_cleanup

**Files:**
- Create: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/action/StagedSemanticCleanupAction.java`
- Create: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/action/StagedSemanticCleanupRequest.java`
- Create: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/action/TransportStagedSemanticCleanupAction.java`
- Create: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/rest/RestStagedSemanticCleanupAction.java`
- Modify: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/InferencePlugin.java`

- [ ] **Step 1: Create ActionType**

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionType;

public class StagedSemanticCleanupAction extends ActionType<StagedSemanticCleanupResponse> {
    public static final StagedSemanticCleanupAction INSTANCE = new StagedSemanticCleanupAction();
    public static final String NAME = "indices:admin/semantic_cleanup";

    private StagedSemanticCleanupAction() {
        super(NAME);
    }
}
```

- [ ] **Step 2: Create Request and Response classes**

`StagedSemanticCleanupRequest.java`:
```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

public class StagedSemanticCleanupRequest extends BroadcastRequest<StagedSemanticCleanupRequest> {

    @Nullable
    private final String field;
    @Nullable
    private final TimeValue maxAge;

    public StagedSemanticCleanupRequest(String[] indices, @Nullable String field, @Nullable TimeValue maxAge) {
        super(indices);
        this.field = field;
        this.maxAge = maxAge;
    }

    public StagedSemanticCleanupRequest(StreamInput in) throws IOException {
        super(in);
        this.field = in.readOptionalString();
        this.maxAge = in.readOptionalTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(field);
        out.writeOptionalTimeValue(maxAge);
    }

    @Nullable
    public String getField() { return field; }

    @Nullable
    public TimeValue getMaxAge() { return maxAge; }
}
```

Create a corresponding `StagedSemanticCleanupResponse.java` following the `BroadcastResponse` pattern used by `ForceMergeAction`. It should include `cleared` (int) and `failed` (int) counts.

- [ ] **Step 3: Create REST handler**

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.inference.action.StagedSemanticCleanupAction;
import org.elasticsearch.xpack.inference.action.StagedSemanticCleanupRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestStagedSemanticCleanupAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "staged_semantic_cleanup_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_semantic_cleanup"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String field = request.param("field");
        TimeValue maxAge = request.paramAsTime("max_age", null);
        StagedSemanticCleanupRequest cleanupRequest = new StagedSemanticCleanupRequest(indices, field, maxAge);
        return channel -> client.execute(StagedSemanticCleanupAction.INSTANCE, cleanupRequest, new RestToXContentListener<>(channel));
    }
}
```

- [ ] **Step 4: Create Transport action stub**

Create `TransportStagedSemanticCleanupAction.java` following the `TransportForceMergeAction` pattern. The `shardOperation()` method should:
1. Get the `IndexShard` from `IndicesService`
2. Scan documents on the shard for `_inference_metadata.*.\_staged` entries
3. For each, check if `now - last_modified > maxAge` (from request or index setting)
4. Clear expired `_staged` sections via internal index operations
5. Return counts

This is a complex transport action. For the initial implementation, use a simple approach: use `UpdateByQueryRequest` internally to find and update documents with expired `_staged` data. The full implementation can be refined later.

- [ ] **Step 5: Register in InferencePlugin**

In `InferencePlugin.java`, register:
- The `RestStagedSemanticCleanupAction` in `getRestHandlers()`
- The `TransportStagedSemanticCleanupAction` in `getActions()`
- The `INDEX_SEMANTIC_TEXT_STAGED_TTL` setting in `getSettings()`

Look for existing registrations of REST handlers and transport actions in `InferencePlugin.java` and follow the same pattern.

- [ ] **Step 6: Run spotless and commit**

```bash
./gradlew :x-pack:plugin:inference:spotlessApply
git add x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/action/StagedSemanticCleanupAction.java \
        x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/action/StagedSemanticCleanupRequest.java \
        x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/action/TransportStagedSemanticCleanupAction.java \
        x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/rest/RestStagedSemanticCleanupAction.java \
        x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/InferencePlugin.java
git commit -m "Add _semantic_cleanup REST endpoint and transport action"
```

---

## Task 11: Background Cleanup Task

**Files:**
- Create: `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/action/AsyncStagedSemanticCleanupTask.java`

- [ ] **Step 1: Implement the background task**

Follow the `AsyncTrimTranslogTask` pattern in `IndexService.java` (lines 1275-1301). The task:
1. Extends `AbstractAsyncTask` (or the `IndexService.BaseAsyncTask` pattern if running per-index)
2. Runs at the `staged_ttl` interval
3. In `runInternal()`, scans for documents with expired `_staged` and clears them
4. Logs at WARN when clearing stale data

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * Periodic background task that scans for and clears stale staged semantic_text data
 * based on the {@code index.semantic_text.staged_ttl} setting.
 */
public class AsyncStagedSemanticCleanupTask extends AbstractAsyncTask {

    private static final Logger logger = LogManager.getLogger(AsyncStagedSemanticCleanupTask.class);

    private final IndexSettings indexSettings;

    public AsyncStagedSemanticCleanupTask(IndexSettings indexSettings, ThreadPool threadPool) {
        super(
            logger,
            threadPool,
            threadPool.generic(),
            indexSettings.getSemanticTextStagedTtl(),
            true
        );
    }

    @Override
    protected boolean mustReschedule() {
        return indexSettings.getSemanticTextStagedTtlMillis() > 0;
    }

    @Override
    protected void runInternal() {
        long ttlMillis = indexSettings.getSemanticTextStagedTtlMillis();
        if (ttlMillis <= 0) {
            return; // TTL disabled
        }
        // Implementation: use the same logic as TransportStagedSemanticCleanupAction
        // to find and clear expired _staged entries on this node's shards.
        // Log cleared entries at WARN level.
        logger.debug("Running staged semantic_text cleanup with TTL [{}ms]", ttlMillis);
    }
}
```

Note: The `getSemanticTextStagedTtl()` method needs to return a `TimeValue`, not just millis. Add an appropriate accessor to `IndexSettings` if the existing one only returns millis. Check how `getTranslogSyncInterval()` (which returns `TimeValue`) is implemented and follow the same pattern.

- [ ] **Step 2: Register the task in InferencePlugin or IndexService**

The background task needs to be created per-index. The cleanest integration point depends on whether the inference plugin can hook into index lifecycle. Explore how `AsyncTrimTranslogTask` is created in `IndexService.java` constructor and follow a similar pattern. If the inference plugin cannot hook into `IndexService` directly, use the `IndexEventListener` interface to create/destroy the task on index create/delete.

- [ ] **Step 3: Run spotless and commit**

```bash
./gradlew :x-pack:plugin:inference:spotlessApply
git add x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/action/AsyncStagedSemanticCleanupTask.java
git commit -m "Add background cleanup task for stale staged semantic data"
```

---

## Task 12: Integration Test — Full BYO Lifecycle

**Files:**
- Create: `x-pack/plugin/inference/src/internalClusterTest/java/org/elasticsearch/xpack/inference/action/filter/ShardBulkInferenceActionFilterBYOIT.java`

- [ ] **Step 1: Write integration test**

This integration test verifies the full BYO lifecycle against a running Elasticsearch node. Follow the pattern in `ShardBulkInferenceActionFilterIT.java`:

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

// Use ESSingleNodeTestCase or ESIntegTestCase following existing IT patterns

/**
 * Integration tests for the BYO chunks and vectors feature.
 * Tests the full lifecycle against a running Elasticsearch node.
 */
public class ShardBulkInferenceActionFilterBYOIT extends ... {

    /**
     * Tests: Create index with semantic_text field, configure inference endpoint,
     * index a BYO document (single-shot), verify it's searchable via semantic_query.
     */
    public void testSingleShotBYOIndexAndSearch() throws Exception {
        // 1. Create inference endpoint (mock)
        // 2. Create index with semantic_text field pointing to endpoint
        // 3. Index document with BYO text + chunks + embeddings
        // 4. Refresh
        // 5. Search with semantic_query — verify hit returned
        // 6. Verify _source contains original field value
    }

    /**
     * Tests: Full multi-part lifecycle — stage_init, stage, commit, search.
     */
    public void testMultiPartStagingLifecycle() throws Exception {
        // 1. Setup index + endpoint
        // 2. stage_init with text + first batch of chunks
        // 3. Search — verify NO results (staged, not committed)
        // 4. stage with more chunks
        // 5. Search — still no results
        // 6. commit
        // 7. Search — verify hit returned
    }

    /**
     * Tests: Cancel discards staged data.
     */
    public void testCancelDiscardsStaged() throws Exception {
        // 1. Setup + stage_init + stage
        // 2. cancel
        // 3. Search — no results
        // 4. Verify document has no _staged in _inference_metadata
    }

    /**
     * Tests: _semantic_cleanup endpoint works.
     */
    public void testSemanticCleanupEndpoint() throws Exception {
        // 1. Setup + stage_init (creates _staged with last_modified = now)
        // 2. POST /{index}/_semantic_cleanup?max_age=0s
        // 3. Verify _staged is cleared
    }

    /**
     * Tests: Backward compatibility — string value still triggers inference.
     */
    public void testStringValueStillUsesInference() throws Exception {
        // 1. Setup index + endpoint
        // 2. Index document with plain string
        // 3. Verify inference was called (embeddings generated)
        // 4. Search — verify hit returned
    }
}
```

- [ ] **Step 2: Run integration tests**

Run: `./gradlew :x-pack:plugin:inference:internalClusterTest --tests "org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilterBYOIT" -Dtests.iters=1`
Expected: All tests PASS.

- [ ] **Step 3: Run spotless and commit**

```bash
./gradlew :x-pack:plugin:inference:spotlessApply
git add x-pack/plugin/inference/src/internalClusterTest/java/org/elasticsearch/xpack/inference/action/filter/ShardBulkInferenceActionFilterBYOIT.java
git commit -m "Add integration tests for BYO chunks and vectors lifecycle"
```

---

## Task 13: YAML REST Test

**Files:**
- Create: `x-pack/plugin/inference/src/yamlRestTest/resources/rest-api-spec/test/inference/semantic_text_byo.yml`

- [ ] **Step 1: Write YAML REST test for BYO single-shot**

```yaml
---
"BYO single-shot semantic_text indexing":
  - do:
      indices.create:
        index: byo-test
        body:
          mappings:
            properties:
              content:
                type: semantic_text
                inference_id: test-inference

  - do:
      index:
        index: byo-test
        id: "1"
        body:
          title: "test document"
          content:
            text: "hello world"
            chunks:
              - start_offset: 0
                end_offset: 5
                embeddings: [0.1, 0.2, 0.3]
              - start_offset: 5
                end_offset: 11
                embeddings: [0.4, 0.5, 0.6]

  - do:
      indices.refresh:
        index: byo-test

  - do:
      search:
        index: byo-test
        body:
          query:
            semantic:
              field: content
              query: "hello"
  - match: { hits.total.value: 1 }
```

Note: This YAML test requires a mock inference endpoint. Consult existing YAML tests in `x-pack/plugin/inference/src/yamlRestTest/resources/rest-api-spec/test/inference/` for how they set up test inference endpoints. Some tests use the `_inference/_test` endpoint pattern.

- [ ] **Step 2: Run YAML REST test**

Run: `./gradlew ":x-pack:plugin:inference:yamlRestTest" --tests "org.elasticsearch.xpack.inference.rest.ServerSentEventsRestActionIT.test {yaml=inference/semantic_text_byo}"`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add x-pack/plugin/inference/src/yamlRestTest/resources/rest-api-spec/test/inference/semantic_text_byo.yml
git commit -m "Add YAML REST tests for BYO semantic_text"
```

---

## Task 14: Final Verification

- [ ] **Step 1: Run all inference plugin tests**

Run: `./gradlew :x-pack:plugin:inference:test -Dtests.iters=1`
Expected: All PASS.

- [ ] **Step 2: Run spotless check**

Run: `./gradlew :x-pack:plugin:inference:spotlessJavaCheck`
Expected: PASS.

- [ ] **Step 3: Run server tests (for new setting)**

Run: `./gradlew :server:test --tests "org.elasticsearch.index.IndexSettingsTests" -Dtests.iters=1`
Expected: PASS.

- [ ] **Step 4: Run internalClusterTests**

Run: `./gradlew :x-pack:plugin:inference:internalClusterTest -Dtests.iters=1`
Expected: PASS.

---

## Reference: Key Files and Line Numbers

| File | Key Lines | What's There |
|------|-----------|-------------|
| `SemanticTextField.java` | 62-68 | Record definition |
| `SemanticTextField.java` | 84-89 | InferenceResult record |
| `SemanticTextField.java` | 96-172 | Chunk class with 3 constructors |
| `SemanticTextField.java` | 236-288 | toXContent serialization |
| `SemanticTextField.java` | 291-308 | Parser definition |
| `ShardBulkInferenceActionFilter.java` | 625-641 | Existing pre-computed bypass logic |
| `ShardBulkInferenceActionFilter.java` | 686-699 | Model settings resolution |
| `ShardBulkInferenceActionFilter.java` | 897-951 | applyInferenceResponses |
| `ShardBulkInferenceActionFilter.java` | 996-1020 | appendSourceAndInferenceMetadata |
| `ShardBulkInferenceActionFilterTests.java` | 1137-1321 | createFilter helper + test setup |
| `ShardBulkInferenceActionFilterTests.java` | 1445-1475 | assertInferenceResults helper |
| `SemanticTextFieldTests.java` | randomSemanticText() | Test helper for creating random data |
| `IndexSettings.java` | 433-440 | INDEX_GC_DELETES_SETTING pattern |
| `IndexScopedSettings.java` | 61-200 | BUILT_IN_INDEX_SETTINGS set |
| `IndexService.java` | 1275-1301 | AsyncTrimTranslogTask pattern |
| `RestForceMergeAction.java` | 40-42 | REST route definition pattern |
| `InferenceMetadataFieldsMapper.java` | 77 | `_inference_fields` field name |
