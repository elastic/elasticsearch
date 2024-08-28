/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.type.EsFieldTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.elasticsearch.test.ByteSizeEqualsMatcher.byteSizeEquals;

public class EsIndexSerializationTests extends AbstractWireSerializingTestCase<EsIndex> {
    public static EsIndex randomEsIndex() {
        String name = randomAlphaOfLength(5);
        Map<String, EsField> mapping = randomMapping();
        Set<String> concreteIndices = randomConcreteIndices();
        return new EsIndex(name, mapping, concreteIndices);
    }

    private static Map<String, EsField> randomMapping() {
        int size = between(0, 10);
        Map<String, EsField> result = new HashMap<>(size);
        while (result.size() < size) {
            result.put(randomAlphaOfLength(5), EsFieldTests.randomAnyEsField(1));
        }
        return result;
    }

    private static Set<String> randomConcreteIndices() {
        int size = between(0, 10);
        Set<String> result = new HashSet<>(size);
        while (result.size() < size) {
            result.add(randomAlphaOfLength(5));
        }
        return result;
    }

    @Override
    protected Writeable.Reader<EsIndex> instanceReader() {
        return a -> new EsIndex(new PlanStreamInput(a, new PlanNameRegistry(), a.namedWriteableRegistry(), null));
    }

    @Override
    protected Writeable.Writer<EsIndex> instanceWriter() {
        return (out, idx) -> new PlanStreamOutput(out, new PlanNameRegistry(), null).writeWriteable(idx);
    }

    @Override
    protected EsIndex createTestInstance() {
        return randomEsIndex();
    }

    @Override
    protected EsIndex mutateInstance(EsIndex instance) throws IOException {
        String name = instance.name();
        Map<String, EsField> mapping = instance.mapping();
        Set<String> concreteIndices = instance.concreteIndices();
        switch (between(0, 2)) {
            case 0 -> name = randomValueOtherThan(name, () -> randomAlphaOfLength(5));
            case 1 -> mapping = randomValueOtherThan(mapping, EsIndexSerializationTests::randomMapping);
            case 2 -> concreteIndices = randomValueOtherThan(concreteIndices, EsIndexSerializationTests::randomConcreteIndices);
            default -> throw new IllegalArgumentException();
        }
        return new EsIndex(name, mapping, concreteIndices);
    }

    /**
     * Build an {@link EsIndex} with many conflicting fields across many indices.
     */
    public static EsIndex indexWithManyConflicts(boolean withParent) {
        /*
         * The number of fields with a mapping conflict.
         */
        int conflictingCount = 250;
        /*
         * The number of indices that map conflicting fields are "keyword".
         * One other index will map the field as "text"
         */
        int keywordIndicesCount = 600;
        /*
         * The number of fields that don't have a mapping conflict.
         */
        int nonConflictingCount = 7000;

        Set<String> keywordIndices = new TreeSet<>();
        for (int i = 0; i < keywordIndicesCount; i++) {
            keywordIndices.add(String.format(Locale.ROOT, ".ds-logs-apache.access-external-2024.08.09-%08d", i));
        }

        Set<String> textIndices = Set.of("logs-endpoint.events.imported");

        Map<String, EsField> fields = new TreeMap<>();
        for (int i = 0; i < conflictingCount; i++) {
            String name = String.format(Locale.ROOT, "blah.blah.blah.blah.blah.blah.conflict.name%04d", i);
            Map<String, Set<String>> conflicts = Map.of("text", textIndices, "keyword", keywordIndices);
            fields.put(name, new InvalidMappedField(name, conflicts));
        }
        for (int i = 0; i < nonConflictingCount; i++) {
            String name = String.format(Locale.ROOT, "blah.blah.blah.blah.blah.blah.nonconflict.name%04d", i);
            fields.put(name, new EsField(name, DataType.KEYWORD, Map.of(), true));
        }

        if (withParent) {
            EsField parent = new EsField("parent", DataType.OBJECT, Map.copyOf(fields), false);
            fields.put("parent", parent);
        }

        TreeSet<String> concrete = new TreeSet<>();
        concrete.addAll(keywordIndices);
        concrete.addAll(textIndices);

        return new EsIndex("name", fields, concrete);
    }

    /**
     * Test the size of serializing an index with many conflicts at the root level.
     * See {@link #testManyTypeConflicts(boolean, ByteSizeValue)} for more.
     */
    public void testManyTypeConflicts() throws IOException {
        testManyTypeConflicts(false, ByteSizeValue.ofBytes(991027));
        /*
         * History:
         *  953.7kb - shorten error messages for UnsupportedAttributes #111973
         *  967.7kb - cache EsFields #112008 (little overhead of the cache)
         */
    }

    /**
     * Test the size of serializing an index with many conflicts inside a "parent" object.
     * See {@link #testManyTypeConflicts(boolean, ByteSizeValue)} for more.
     */
    public void testManyTypeConflictsWithParent() throws IOException {
        testManyTypeConflicts(true, ByteSizeValue.ofBytes(1374498));
        /*
         * History:
         * 16.9mb - start
         *  1.8mb - shorten error messages for UnsupportedAttributes #111973
         *  1.3mb - cache EsFields #112008
         */
    }

    /**
     * Test the size of serializing an index with many conflicts. Callers of
     * this method intentionally use a very precise size for the serialized
     * data so a programmer making changes has to think when this size changes.
     * <p>
     *     In general, shrinking the over the wire size is great and the precise
     *     size should just ratchet downwards. Small upwards movement is fine so
     *     long as you understand why the change is happening and you think it's
     *     worth it for the data node request for a big index to grow.
     * </p>
     * <p>
     *     Large upwards movement in the size is not fine! Folks frequently make
     *     requests across large clusters with many fields and these requests can
     *     really clog up the network interface. Super large results here can make
     *     ESQL impossible to use at all for big mappings with many conflicts.
     * </p>
     */
    private void testManyTypeConflicts(boolean withParent, ByteSizeValue expected) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput(); var pso = new PlanStreamOutput(out, new PlanNameRegistry(), null)) {
            indexWithManyConflicts(withParent).writeTo(pso);
            assertThat(ByteSizeValue.ofBytes(out.bytes().length()), byteSizeEquals(expected));
        }
    }
}
