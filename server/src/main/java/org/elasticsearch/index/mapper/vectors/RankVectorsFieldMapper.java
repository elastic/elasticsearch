/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.ArraySourceValueFetcher;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT_BIT;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.namesToElementType;

public class RankVectorsFieldMapper extends FieldMapper {

    public static final String VECTOR_MAGNITUDES_SUFFIX = "._magnitude";
    public static final FeatureFlag FEATURE_FLAG = new FeatureFlag("rank_vectors");
    public static final String CONTENT_TYPE = "rank_vectors";

    private static RankVectorsFieldMapper toType(FieldMapper in) {
        return (RankVectorsFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<DenseVectorFieldMapper.ElementType> elementType = new Parameter<>(
            "element_type",
            false,
            () -> DenseVectorFieldMapper.ElementType.FLOAT,
            (n, c, o) -> {
                DenseVectorFieldMapper.ElementType elementType = namesToElementType.get((String) o);
                if (elementType == null) {
                    throw new MapperParsingException(
                        "invalid element_type [" + o + "]; available types are " + namesToElementType.keySet()
                    );
                }
                return elementType;
            },
            m -> toType(m).fieldType().elementType,
            XContentBuilder::field,
            Objects::toString
        );

        // This is defined as updatable because it can be updated once, from [null] to a valid dim size,
        // by a dynamic mapping update. Once it has been set, however, the value cannot be changed.
        private final Parameter<Integer> dims = new Parameter<>("dims", true, () -> null, (n, c, o) -> {
            if (o instanceof Integer == false) {
                throw new MapperParsingException("Property [dims] on field [" + n + "] must be an integer but got [" + o + "]");
            }

            return XContentMapValues.nodeIntegerValue(o);
        }, m -> toType(m).fieldType().dims, XContentBuilder::field, Object::toString).setSerializerCheck((id, ic, v) -> v != null)
            .setMergeValidator((previous, current, c) -> previous == null || Objects.equals(previous, current))
            .addValidator(dims -> {
                if (dims == null) {
                    return;
                }
                int maxDims = elementType.getValue() == DenseVectorFieldMapper.ElementType.BIT ? MAX_DIMS_COUNT_BIT : MAX_DIMS_COUNT;
                int minDims = elementType.getValue() == DenseVectorFieldMapper.ElementType.BIT ? Byte.SIZE : 1;
                if (dims < minDims || dims > maxDims) {
                    throw new MapperParsingException(
                        "The number of dimensions should be in the range [" + minDims + ", " + maxDims + "] but was [" + dims + "]"
                    );
                }
                if (elementType.getValue() == DenseVectorFieldMapper.ElementType.BIT) {
                    if (dims % Byte.SIZE != 0) {
                        throw new MapperParsingException("The number of dimensions for should be a multiple of 8 but was [" + dims + "]");
                    }
                }
            });
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final IndexVersion indexCreatedVersion;

        public Builder(String name, IndexVersion indexCreatedVersion) {
            super(name);
            this.indexCreatedVersion = indexCreatedVersion;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { elementType, dims, meta };
        }

        public RankVectorsFieldMapper.Builder dimensions(int dimensions) {
            this.dims.setValue(dimensions);
            return this;
        }

        public RankVectorsFieldMapper.Builder elementType(DenseVectorFieldMapper.ElementType elementType) {
            this.elementType.setValue(elementType);
            return this;
        }

        @Override
        public RankVectorsFieldMapper build(MapperBuilderContext context) {
            // Validate again here because the dimensions or element type could have been set programmatically,
            // which affects index option validity
            validate();
            return new RankVectorsFieldMapper(
                leafName(),
                new RankVectorsFieldType(
                    context.buildFullName(leafName()),
                    elementType.getValue(),
                    dims.getValue(),
                    indexCreatedVersion,
                    meta.getValue()
                ),
                builderParams(this, context),
                indexCreatedVersion
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser(
        (n, c) -> new RankVectorsFieldMapper.Builder(n, c.indexVersionCreated()),
        notInMultiFields(CONTENT_TYPE)
    );

    public static final class RankVectorsFieldType extends SimpleMappedFieldType {
        private final DenseVectorFieldMapper.ElementType elementType;
        private final Integer dims;
        private final IndexVersion indexCreatedVersion;

        public RankVectorsFieldType(
            String name,
            DenseVectorFieldMapper.ElementType elementType,
            Integer dims,
            IndexVersion indexCreatedVersion,
            Map<String, String> meta
        ) {
            super(name, false, false, true, TextSearchInfo.NONE, meta);
            this.elementType = elementType;
            this.dims = dims;
            this.indexCreatedVersion = indexCreatedVersion;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            return new ArraySourceValueFetcher(name(), context) {
                @Override
                protected Object parseSourceValue(Object value) {
                    return value;
                }
            };
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            throw new IllegalArgumentException(
                "Field [" + name() + "] of type [" + typeName() + "] doesn't support docvalue_fields or aggregations"
            );
        }

        @Override
        public boolean isAggregatable() {
            return false;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            return new RankVectorsIndexFieldData.Builder(name(), CoreValuesSourceType.KEYWORD, indexCreatedVersion, dims, elementType);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return new FieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support term queries");
        }

        int getVectorDimensions() {
            return dims;
        }

        DenseVectorFieldMapper.ElementType getElementType() {
            return elementType;
        }
    }

    private final IndexVersion indexCreatedVersion;

    private RankVectorsFieldMapper(String simpleName, MappedFieldType fieldType, BuilderParams params, IndexVersion indexCreatedVersion) {
        super(simpleName, fieldType, params);
        this.indexCreatedVersion = indexCreatedVersion;
    }

    @Override
    public RankVectorsFieldType fieldType() {
        return (RankVectorsFieldType) super.fieldType();
    }

    @Override
    public boolean parsesArrayValue() {
        return true;
    }

    @Override
    public void parse(DocumentParserContext context) throws IOException {
        if (context.doc().getByKey(fieldType().name()) != null) {
            throw new IllegalArgumentException(
                "Field ["
                    + fullPath()
                    + "] of type ["
                    + typeName()
                    + "] doesn't support indexing multiple values for the same field in the same document"
            );
        }
        if (XContentParser.Token.VALUE_NULL == context.parser().currentToken()) {
            return;
        }
        if (XContentParser.Token.START_ARRAY != context.parser().currentToken()) {
            throw new IllegalArgumentException(
                "Field [" + fullPath() + "] of type [" + typeName() + "] cannot be indexed with a single value"
            );
        }
        if (fieldType().dims == null) {
            int currentDims = -1;
            while (XContentParser.Token.END_ARRAY != context.parser().nextToken()) {
                int dims = fieldType().elementType.parseDimensionCount(context);
                if (currentDims == -1) {
                    currentDims = dims;
                } else if (currentDims != dims) {
                    throw new IllegalArgumentException(
                        "Field [" + fullPath() + "] of type [" + typeName() + "] cannot be indexed with vectors of different dimensions"
                    );
                }
            }
            RankVectorsFieldType updatedFieldType = new RankVectorsFieldType(
                fieldType().name(),
                fieldType().elementType,
                currentDims,
                indexCreatedVersion,
                fieldType().meta()
            );
            Mapper update = new RankVectorsFieldMapper(leafName(), updatedFieldType, builderParams, indexCreatedVersion);
            context.addDynamicMapper(update);
            return;
        }
        int dims = fieldType().dims;
        DenseVectorFieldMapper.ElementType elementType = fieldType().elementType;
        List<VectorData> vectors = new ArrayList<>();
        while (XContentParser.Token.END_ARRAY != context.parser().nextToken()) {
            VectorData vector = elementType.parseKnnVector(context, dims, (i, b) -> {
                if (b) {
                    checkDimensionMatches(i, context);
                } else {
                    checkDimensionExceeded(i, context);
                }
            }, null);
            vectors.add(vector);
        }
        int bufferSize = elementType.getNumBytes(dims) * vectors.size();
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer magnitudeBuffer = ByteBuffer.allocate(vectors.size() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (VectorData vector : vectors) {
            vector.addToBuffer(buffer);
            magnitudeBuffer.putFloat((float) Math.sqrt(elementType.computeSquaredMagnitude(vector)));
        }
        String vectorFieldName = fieldType().name();
        String vectorMagnitudeFieldName = vectorFieldName + VECTOR_MAGNITUDES_SUFFIX;
        context.doc().addWithKey(vectorFieldName, new BinaryDocValuesField(vectorFieldName, new BytesRef(buffer.array())));
        context.doc()
            .addWithKey(
                vectorMagnitudeFieldName,
                new BinaryDocValuesField(vectorMagnitudeFieldName, new BytesRef(magnitudeBuffer.array()))
            );
    }

    private void checkDimensionExceeded(int index, DocumentParserContext context) {
        if (index >= fieldType().dims) {
            throw new IllegalArgumentException(
                "The ["
                    + typeName()
                    + "] field ["
                    + fullPath()
                    + "] in doc ["
                    + context.documentDescription()
                    + "] has more dimensions "
                    + "than defined in the mapping ["
                    + fieldType().dims
                    + "]"
            );
        }
    }

    private void checkDimensionMatches(int index, DocumentParserContext context) {
        if (index != fieldType().dims) {
            throw new IllegalArgumentException(
                "The ["
                    + typeName()
                    + "] field ["
                    + fullPath()
                    + "] in doc ["
                    + context.documentDescription()
                    + "] has a different number of dimensions "
                    + "["
                    + index
                    + "] than defined in the mapping ["
                    + fieldType().dims
                    + "]"
            );
        }
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {
        throw new AssertionError("parse is implemented directly");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new RankVectorsFieldMapper.Builder(leafName(), indexCreatedVersion).init(this);
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport.Native(new RankVectorsFieldMapper.DocValuesSyntheticFieldLoader());
    }

    private class DocValuesSyntheticFieldLoader extends SourceLoader.DocValuesBasedSyntheticFieldLoader {
        private BinaryDocValues values;
        private boolean hasValue;

        @Override
        public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
            values = leafReader.getBinaryDocValues(fullPath());
            if (values == null) {
                return null;
            }
            return docId -> {
                hasValue = docId == values.advance(docId);
                return hasValue;
            };
        }

        @Override
        public boolean hasValue() {
            return hasValue;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            if (false == hasValue) {
                return;
            }
            b.startArray(leafName());
            BytesRef ref = values.binaryValue();
            ByteBuffer byteBuffer = ByteBuffer.wrap(ref.bytes, ref.offset, ref.length).order(ByteOrder.LITTLE_ENDIAN);
            assert ref.length % fieldType().elementType.getNumBytes(fieldType().dims) == 0;
            int numVecs = ref.length / fieldType().elementType.getNumBytes(fieldType().dims);
            for (int i = 0; i < numVecs; i++) {
                b.startArray();
                int dims = fieldType().elementType == DenseVectorFieldMapper.ElementType.BIT
                    ? fieldType().dims / Byte.SIZE
                    : fieldType().dims;
                for (int dim = 0; dim < dims; dim++) {
                    fieldType().elementType.readAndWriteValue(byteBuffer, b);
                }
                b.endArray();
            }
            b.endArray();
        }

        @Override
        public String fieldName() {
            return fullPath();
        }
    }
}
