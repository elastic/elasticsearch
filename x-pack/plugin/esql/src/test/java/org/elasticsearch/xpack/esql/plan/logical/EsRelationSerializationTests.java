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
            randomSlices()
        );
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
        switch (between(0, 6)) {
            case 0 -> indexPattern = randomValueOtherThan(indexPattern, ESTestCase::randomIdentifier);
            case 1 -> indexMode = randomValueOtherThan(indexMode, () -> randomFrom(IndexMode.availableModes()));
            case 2 -> indexProperties = randomValueOtherThan(indexProperties, EsIndexGenerator::randomIndexProperties);
            case 3 -> originalIndices = randomValueOtherThan(originalIndices, EsIndexGenerator::randomRemotesWithIndices);
            case 4 -> concreteIndices = randomValueOtherThan(concreteIndices, EsIndexGenerator::randomRemotesWithIndices);
            case 5 -> attributes = randomValueOtherThan(attributes, () -> randomFieldAttributes(0, 10, false));
            case 6 -> slices = randomValueOtherThan(slices, EsRelationSerializationTests::randomSlices);
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
            slices
        );
    }

    /**
     * A node that cannot read the slice selection reads every slice. The selection is only ever derived from a filter that
     * the node receives with the plan, so the relation is sent without it.
     */
    public void testSliceSelectionIsDroppedForNodesThatCannotReadIt() throws IOException {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(EsRelation.SLICES);
        EsRelation relation = withSlices(randomEsRelation(), SliceSelection.of(List.of("s1")));
        assertThat(copyInstance(relation, before), equalTo(withSlices(relation, SliceSelection.UNSPECIFIED)));
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
            slices
        );
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
