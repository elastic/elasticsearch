/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TemporalityAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.plan.physical.FetchBoundaryExec;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/** Tests the semantic and wire contracts of {@link FieldExtractionSpec}. */
public class FieldExtractionSpecTests extends ESTestCase {

    public void testDirectSpecificationRoundTrip() throws IOException {
        FieldExtractionSpec original = FieldExtractionSpec.direct(
            "salary",
            DataType.INTEGER,
            MappedFieldType.FieldExtractPreference.STORED
        );

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.current());
            original.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                in.setTransportVersion(TransportVersion.current());
                assertThat(new FieldExtractionSpec(in), equalTo(original));
            }
        }
    }

    public void testPlansCompleteDirectSpecification() {
        FieldAttribute attribute = new FieldAttribute(
            Source.EMPTY,
            "salary",
            new EsField("salary", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );

        FieldExtractionSpec spec = FieldExtractionSpec.plan(attribute, MappedFieldType.FieldExtractPreference.STORED).orElseThrow();

        assertThat(spec.operation(), equalTo(FieldExtractionSpec.Operation.DIRECT));
        assertThat(spec.fieldName(), equalTo("salary"));
        assertThat(spec.dataType(), equalTo(DataType.INTEGER));
        assertThat(spec.elementType(), equalTo(ElementType.INT));
        assertThat(spec.fieldExtractPreference(), equalTo(MappedFieldType.FieldExtractPreference.STORED));
        assertThat(spec.missingFieldPolicy(), equalTo(FieldExtractionSpec.MissingFieldPolicy.NULL));
    }

    public void testDoesNotInventExtractionSemanticsForReferenceAttribute() {
        ReferenceAttribute attribute = new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.INTEGER);

        assertTrue(FieldExtractionSpec.plan(attribute, MappedFieldType.FieldExtractPreference.NONE).isEmpty());
    }

    public void testDoesNotPlanAttributesThatNeedSpecializedExtraction() {
        List<Attribute> attributes = List.of(
            new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("pod")),
            new TemporalityAttribute(Source.EMPTY),
            field(
                new FunctionEsField(
                    new EsField("specialized", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
                    DataType.INTEGER,
                    new BlockLoaderFunctionConfig.JustFunction(BlockLoaderFunctionConfig.Function.LENGTH)
                )
            ),
            field(new PotentiallyUnmappedKeywordEsField("specialized")),
            field(new MultiTypeEsField("specialized", DataType.DATE_NANOS, true, Map.of(), EsField.TimeSeriesFieldType.NONE, null))
        );

        for (Attribute attribute : attributes) {
            assertTrue(attribute.toString(), FieldExtractionSpec.isSupportedByDirectOperation(attribute.dataType()));
            assertTrue(attribute.toString(), FieldExtractionSpec.plan(attribute, MappedFieldType.FieldExtractPreference.NONE).isEmpty());
        }
    }

    public void testDoesNotPlanTypesThatNeedSpecializedExtraction() {
        List<DataType> dataTypes = List.of(
            DataType.SHORT,
            DataType.BYTE,
            DataType.FLOAT,
            DataType.HALF_FLOAT,
            DataType.SCALED_FLOAT,
            DataType.GEO_POINT,
            DataType.CARTESIAN_POINT,
            DataType.CARTESIAN_SHAPE,
            DataType.GEO_SHAPE
        );

        for (DataType dataType : dataTypes) {
            assertFalse(dataType.typeName(), FieldExtractionSpec.isSupportedByDirectOperation(dataType));
            assertTrue(
                dataType.typeName(),
                FieldExtractionSpec.plan(
                    field(new EsField("specialized", dataType, Map.of(), true, EsField.TimeSeriesFieldType.NONE)),
                    MappedFieldType.FieldExtractPreference.NONE
                ).isEmpty()
            );
        }
    }

    public void testDirectOperationUsesFetchBoundaryCompatibility() {
        FieldExtractionSpec spec = FieldExtractionSpec.direct("salary", DataType.INTEGER);

        assertFalse(spec.supports(TransportVersionUtils.getPreviousVersion(FetchBoundaryExec.ESQL_FETCH_BOUNDARY)));
        assertTrue(spec.supports(TransportVersion.current()));
    }

    public void testBindsDirectOperationToShardLoader() {
        FieldExtractionSpec spec = FieldExtractionSpec.direct("salary", DataType.INTEGER, MappedFieldType.FieldExtractPreference.STORED);
        EsPhysicalOperationProviders.ShardContext shardContext = Mockito.mock(EsPhysicalOperationProviders.ShardContext.class);
        BlockLoader blockLoader = Mockito.mock(BlockLoader.class, Mockito.withSettings().stubOnly());
        Mockito.when(
            shardContext.blockLoader(
                "salary",
                false,
                MappedFieldType.FieldExtractPreference.STORED,
                null,
                null,
                PlannerSettings.DEFAULTS.blockLoaderSizeOrdinals(),
                PlannerSettings.DEFAULTS.blockLoaderSizeScript()
            )
        ).thenReturn(blockLoader);

        var bound = spec.bind(shardContext, PlannerSettings.DEFAULTS, null);

        assertSame(blockLoader, bound.loader());
    }

    private static FieldAttribute field(EsField field) {
        return new FieldAttribute(Source.EMPTY, field.getName(), field);
    }
}
