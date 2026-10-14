/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.SliceSelection;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexProperties;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.index.EsIndexGenerator.randomIndexProperties;
import static org.elasticsearch.xpack.esql.index.EsIndexGenerator.randomRemotesWithIndices;
import static org.hamcrest.Matchers.equalTo;

public class EsRelationSerializationTests extends AbstractLogicalPlanSerializationTests<EsRelation> {
    public static EsRelation randomEsRelation() {
        return new EsRelation(
            randomSource(),
            randomIdentifier(),
            randomFrom(IndexMode.availableModes()),
            randomRemotesWithIndices(),
            randomRemotesWithIndices(),
            randomIndexProperties(),
            randomFieldAttributes(0, 10, false),
            randomSlices(),
            randomSingleValuedFields()
        );
    }

    private static Set<String> randomSingleValuedFields() {
        return Set.copyOf(randomList(0, 4, ESTestCase::randomIdentifier));
    }

    private static SliceSelection randomSlices() {
        return randomFrom(
            SliceSelection.UNSPECIFIED,
            SliceSelection.ALL,
            SliceSelection.of(randomList(1, 3, () -> randomAlphaOfLengthBetween(1, 8)))
        );
    }

    @Override
    protected EsRelation createTestInstance() {
        return randomEsRelation();
    }

    @Override
    protected EsRelation mutateInstance(EsRelation instance) throws IOException {
        String indexPattern = instance.indexPattern();
        IndexMode indexMode = instance.indexMode();
        Map<String, List<String>> originalIndices = instance.originalIndices();
        Map<String, List<String>> concreteIndices = instance.concreteIndices();
        Map<String, IndexProperties> indexProperties = instance.indexProperties();
        List<Attribute> attributes = instance.output();
        SliceSelection slices = instance.slices();
        Set<String> singleValuedFields = instance.singleValuedFields();
        switch (between(0, 7)) {
            case 0 -> indexPattern = randomValueOtherThan(indexPattern, ESTestCase::randomIdentifier);
            case 1 -> indexMode = randomValueOtherThan(indexMode, () -> randomFrom(IndexMode.availableModes()));
            case 2 -> indexProperties = randomValueOtherThan(indexProperties, EsIndexGenerator::randomIndexProperties);
            case 3 -> originalIndices = randomValueOtherThan(originalIndices, EsIndexGenerator::randomRemotesWithIndices);
            case 4 -> concreteIndices = randomValueOtherThan(concreteIndices, EsIndexGenerator::randomRemotesWithIndices);
            case 5 -> attributes = randomValueOtherThan(attributes, () -> randomFieldAttributes(0, 10, false));
            case 6 -> slices = randomValueOtherThan(slices, EsRelationSerializationTests::randomSlices);
            case 7 -> singleValuedFields = randomValueOtherThan(singleValuedFields, EsRelationSerializationTests::randomSingleValuedFields);
            default -> throw new IllegalArgumentException();
        }
        return new EsRelation(
            instance.source(),
            indexPattern,
            indexMode,
            originalIndices,
            concreteIndices,
            indexProperties,
            attributes,
            slices,
            singleValuedFields
        );
    }

    /**
     * A node that cannot read the slice selection reads every slice. The selection is only ever derived from a filter that
     * the node receives with the plan, so the relation is sent without it.
     */
    public void testSliceSelectionIsDroppedForNodesThatCannotReadIt() throws IOException {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(EsRelation.SLICES);
        // The set did not exist before the slices either, so it is dropped here too.
        EsRelation relation = withSlices(randomEsRelation().withSingleValuedFields(Set.of()), SliceSelection.of(List.of("s1")));
        assertThat(copyInstance(relation, before), equalTo(withSlices(relation, SliceSelection.UNSPECIFIED)));
    }

    /** A node that cannot read the set sees no guarantees, which is safe: absent means "not guaranteed". */
    public void testSingleValuedFieldsAreDroppedForNodesThatCannotReadThem() throws IOException {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(EsRelation.SINGLE_VALUED_FIELDS);
        EsRelation relation = randomEsRelation().withSingleValuedFields(Set.of("a", "b"));
        assertThat(copyInstance(relation, before), equalTo(relation.withSingleValuedFields(Set.of())));
    }

    private static EsRelation withSlices(EsRelation relation, SliceSelection slices) {
        return new EsRelation(
            relation.source(),
            relation.indexPattern(),
            relation.indexMode(),
            relation.originalIndices(),
            relation.concreteIndices(),
            relation.indexProperties(),
            relation.output(),
            slices,
            relation.singleValuedFields()
        );
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
