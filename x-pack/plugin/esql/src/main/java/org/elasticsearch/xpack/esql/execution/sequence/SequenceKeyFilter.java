/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Owns a bounded snapshot of waiting keys until the candidate search terminates. Only exact original field identities
 * qualify; unsupported or computed components disable the whole optional predicate. This initial variant therefore
 * needs no inverse scalar transformations or lossy tuple projection.
 */
final class SequenceKeyFilter implements Releasable {
    static final int MAX_TUPLES = 128;
    private static final long MAX_BYTES = 256L * 1024;
    private static final String LABEL = "esql_sequence_key_filter";
    private static final Set<DataType> SUPPORTED = Set.of(DataType.BOOLEAN, DataType.INTEGER, DataType.LONG, DataType.KEYWORD);

    private final CircuitBreaker breaker;
    private final Runnable checkCancelled;
    private List<Field> fields;
    private Set<BytesRef> keys;
    private QueryBuilder compiledPredicate;
    private long heldBytes;
    private boolean complete = true;
    private boolean frozen;
    private boolean closed;

    private SequenceKeyFilter(List<Attribute> attributes, List<Attribute> raw, CircuitBreaker breaker, Runnable checkCancelled) {
        this.breaker = breaker;
        this.checkCancelled = checkCancelled;
        // Admit the bounded hash table, field metadata and transient container copies before allocating them.
        long bytes = 16384L + 256L * attributes.size();
        breaker.addEstimateBytesAndMaybeBreak(bytes, LABEL);
        heldBytes = bytes;
        try {
            fields = new ArrayList<>(attributes.size());
            keys = new HashSet<>(256);
            for (Attribute key : attributes) {
                checkCancelled.run();
                if (SUPPORTED.contains(key.dataType()) == false) {
                    return;
                }
                FieldAttribute original = null;
                for (Attribute candidate : raw) {
                    if (candidate instanceof FieldAttribute field && field.id().equals(key.id()) && field.dataType() == key.dataType()) {
                        original = field;
                        break;
                    }
                }
                if (original == null) {
                    return;
                }
                fields.add(new Field(original.fieldName().string(), key.dataType()));
            }
        } catch (RuntimeException | Error failure) {
            close();
            throw failure;
        }
    }

    /** No key, unsupported provenance or excessive metadata means an ordinary unfiltered read. */
    @Nullable
    static SequenceKeyFilter create(List<Attribute> attributes, List<Attribute> raw, CircuitBreaker breaker, Runnable checkCancelled) {
        if (attributes.isEmpty() || 16384L + 256L * attributes.size() > MAX_BYTES) {
            return null;
        }
        SequenceKeyFilter owner = new SequenceKeyFilter(attributes, raw, breaker, checkCancelled);
        if (owner.fields.size() != attributes.size()) {
            owner.close();
            return null;
        }
        return owner;
    }

    /** Copies admitted canonical bytes; false invalidates the whole snapshot instead of publishing a prefix. */
    boolean add(BytesRef key) {
        checkCancelled.run();
        if (closed || frozen) {
            throw new IllegalStateException("SEQUENCE key filter is closed or published");
        }
        if (complete == false) {
            return false;
        }
        if (keys.contains(key)) {
            return true;
        }
        // Includes the copied key, decoded strings, native term values, tuple clauses and construction overlap.
        long bytes = 512L + 192L * fields.size() + 8L * key.length;
        for (Field field : fields) {
            // Serialized tuple clauses repeat physical field names; include their worst-case UTF-8 bytes.
            bytes += 4L * field.name.length();
        }
        if (keys.size() == MAX_TUPLES || bytes > MAX_BYTES - heldBytes) {
            complete = false;
            return false;
        }
        try {
            breaker.addEstimateBytesAndMaybeBreak(bytes, LABEL);
            heldBytes += bytes;
            keys.add(BytesRef.deepCopyOf(key));
            return true;
        } catch (RuntimeException | Error failure) {
            complete = false;
            throw failure;
        }
    }

    /** Builds complete AND tuples, never independent component sets; any conversion failure disables filtering. */
    @Nullable
    QueryBuilder predicate() {
        if (closed) {
            throw new IllegalStateException("SEQUENCE key filter is closed");
        }
        frozen = true;
        if (complete == false) {
            return null;
        }
        if (compiledPredicate != null) {
            return compiledPredicate;
        }
        if (keys.isEmpty() || (long) keys.size() * fields.size() >= Math.min(1024, IndexSearcher.getMaxClauseCount())) {
            return null;
        }
        BoolQueryBuilder alternatives = new BoolQueryBuilder().minimumShouldMatch(1);
        try {
            for (BytesRef key : keys) {
                checkCancelled.run();
                ByteBuffer encoded = ByteBuffer.wrap(key.bytes, key.offset, key.length).slice();
                BoolQueryBuilder tuple = new BoolQueryBuilder();
                for (Field field : fields) {
                    tuple.filter(new TermQueryBuilder(field.name, literal(encoded, field.type)));
                }
                if (encoded.hasRemaining()) {
                    return null;
                }
                alternatives.should(tuple);
            }
        } catch (BufferUnderflowException | IllegalArgumentException invalidLiteral) {
            return null;
        }
        // The projection is the complete canonical tuple, so deduplicating canonical keys also deduplicates predicates.
        compiledPredicate = alternatives;
        return compiledPredicate;
    }

    private static Object literal(ByteBuffer encoded, DataType type) {
        int tag = encoded.get();
        return switch (type) {
            case BOOLEAN -> {
                int value = encoded.get();
                if (tag != 1 || (value != 0 && value != 1)) {
                    throw new IllegalArgumentException("Invalid canonical boolean key");
                }
                yield value == 1;
            }
            case INTEGER -> {
                if (tag != 2) {
                    throw new IllegalArgumentException("Invalid canonical integer key");
                }
                yield encoded.getInt();
            }
            case LONG -> {
                if (tag != 3) {
                    throw new IllegalArgumentException("Invalid canonical long key");
                }
                yield encoded.getLong();
            }
            case KEYWORD -> {
                if (tag != 6) {
                    throw new IllegalArgumentException("Invalid canonical keyword key");
                }
                int length = encoded.getInt();
                if (length < 0 || length > encoded.remaining()) {
                    throw new IllegalArgumentException("Invalid canonical keyword length");
                }
                byte[] bytes = new byte[length];
                encoded.get(bytes);
                String value = new String(bytes, StandardCharsets.UTF_8);
                if (Arrays.equals(bytes, value.getBytes(StandardCharsets.UTF_8)) == false) {
                    throw new IllegalArgumentException("Keyword key does not round-trip");
                }
                yield value;
            }
            default -> throw new IllegalArgumentException("Unexpected canonical key type [" + type + "]");
        };
    }

    /** Returns this snapshot's source admissions once, including an abandoned partial snapshot. */
    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            compiledPredicate = null;
            keys = null;
            fields = null;
            long bytes = heldBytes;
            heldBytes = 0;
            breaker.addWithoutBreaking(-bytes);
        }
    }

    private record Field(String name, DataType type) {}
}
