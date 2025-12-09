/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.ArraySourceValueFetcher;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.Element;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.SyntheticVectorsPatchFieldLoader;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
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
import static org.elasticsearch.xpack.rank.vectors.RankVectorsPlugin.RANK_VECTORS_FEATURE;

public class RankVectorsFieldMapper extends FieldMapper {

    public static final String VECTOR_MAGNITUDES_SUFFIX = "._magnitude";
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
                if (elementType == ElementType.BFLOAT16) {
                    throw new MapperParsingException("Rank vectors does not support bfloat16");
                }
                return elementType;
            },
            m -> toType(m).fieldType().element.elementType(),
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
        }, m -> toType(m).fieldType().dims, XContentBuilder::field, Objects::toString).setSerializerCheck((id, ic, v) -> v != null)
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
        private final XPackLicenseState licenseState;
        private final boolean isExcludeSourceVectors;

        public Builder(String name, IndexVersion indexCreatedVersion, XPackLicenseState licenseState, boolean isExcludeSourceVectors) {
            super(name);
            this.indexCreatedVersion = indexCreatedVersion;
            this.licenseState = licenseState;
            this.isExcludeSourceVectors = isExcludeSourceVectors;
        }

        public Builder dimensions(int dimensions) {
            this.dims.setValue(dimensions);
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { elementType, dims, meta };
        }

        @Override
        public RankVectorsFieldMapper build(MapperBuilderContext context) {
            // Validate on Mapping creation
            if (RANK_VECTORS_FEATURE.check(licenseState) == false) {
                throw LicenseUtils.newComplianceException("Rank Vectors");
            }
            // Validate again here because the dimensions or element type could have been set programmatically,
            // which affects index option validity
            validate();
            boolean isExcludeSourceVectorsFinal = context.isSourceSynthetic() == false && isExcludeSourceVectors;
            return new RankVectorsFieldMapper(
                leafName(),
                new RankVectorsFieldType(
                    context.buildFullName(leafName()),
                    elementType.getValue(),
                    dims.getValue(),
                    licenseState,
                    meta.getValue()
                ),
                builderParams(this, context),
                indexCreatedVersion,
                licenseState,
                isExcludeSourceVectorsFinal
            );
        }
    }

    public static final class RankVectorsFieldType extends SimpleMappedFieldType {
        private final Element element;
        private final Integer dims;
        private final XPackLicenseState licenseState;

        public RankVectorsFieldType(
            String name,
            ElementType elementType,
            Integer dims,
            XPackLicenseState licenseState,
            Map<String, String> meta
        ) {
            super(name, IndexType.docValuesOnly(), false, meta);
            this.element = Element.getElement(elementType);
            this.dims = dims;
            this.licenseState = licenseState;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isSearchable() {
            return false;
        }

        @Override
        public boolean isVectorEmbedding() {
            return true;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            return new ArraySourceValueFetcher(name(), context) {
                @Override
                protected Object parseSourceValue(Object value) {
                    List<?> outerList = (List<?>) value;
                    List<Object> vectors = new ArrayList<>(outerList.size());
                    for (Object o : outerList) {
                        if (o instanceof List<?> innerList) {
                            float[] vector = new float[innerList.size()];
                            for (int i = 0; i < vector.length; i++) {
                                vector[i] = ((Number) innerList.get(i)).floatValue();
                            }
                            vectors.add(vector);
                        } else {
                            vectors.add(o);
                        }
                    }
                    return vectors;
                }
            };
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            return DocValueFormat.DENSE_VECTOR;
        }

        @Override
        public boolean isAggregatable() {
            return false;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            if (RANK_VECTORS_FEATURE.check(licenseState) == false) {
                throw LicenseUtils.newComplianceException("Rank Vectors");
            }
            return new RankVectorsIndexFieldData.Builder(name(), CoreValuesSourceType.KEYWORD, dims, element.elementType());
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
            return element.elementType();
        }
    }

    private final IndexVersion indexCreatedVersion;
    private final XPackLicenseState licenseState;
    private final boolean isExcludeSourceVectors;

    private RankVectorsFieldMapper(
        String simpleName,
        MappedFieldType fieldType,
        BuilderParams params,
        IndexVersion indexCreatedVersion,
        XPackLicenseState licenseState,
        boolean isExcludeSourceVectors
    ) {
        super(simpleName, fieldType, params);
        this.indexCreatedVersion = indexCreatedVersion;
        this.licenseState = licenseState;
        this.isExcludeSourceVectors = isExcludeSourceVectors;
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
        if (RANK_VECTORS_FEATURE.check(licenseState) == false) {
            throw LicenseUtils.newComplianceException("Rank Vectors");
        }
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
                int dims = fieldType().element.parseDimensionCount(context);
                if (currentDims == -1) {
                    currentDims = dims;
                } else if (currentDims != dims) {
                    throw new IllegalArgumentException(
                        "Field [" + fullPath() + "] of type [" + typeName() + "] cannot be indexed with vectors of different dimensions"
                    );
                }
            }
            var builder = (Builder) getMergeBuilder();
            builder.dimensions(currentDims);
            Mapper update = builder.build(context.createDynamicMapperBuilderContext());
            context.addDynamicMapper(update);
            return;
        }
        int dims = fieldType().dims;
        Element element = fieldType().element;
        List<VectorData> vectors = new ArrayList<>();
        while (XContentParser.Token.END_ARRAY != context.parser().nextToken()) {
            VectorData vector = element.parseKnnVector(context, dims, (i, b) -> {
                if (b) {
                    checkDimensionMatches(i, context);
                } else {
                    checkDimensionExceeded(i, context);
                }
            }, null);
            vectors.add(vector);
        }
        int bufferSize = element.getNumBytes(dims) * vectors.size();
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer magnitudeBuffer = ByteBuffer.allocate(vectors.size() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (VectorData vector : vectors) {
            vector.addToBuffer(element, buffer);
            magnitudeBuffer.putFloat((float) Math.sqrt(element.computeSquaredMagnitude(vector)));
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
        return new Builder(leafName(), indexCreatedVersion, licenseState, isExcludeSourceVectors).init(this);
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport.Native(DocValuesSyntheticFieldLoader::new);
    }

    @Override
    public SourceLoader.SyntheticVectorsLoader syntheticVectorsLoader() {
        if (isExcludeSourceVectors) {
            return new SyntheticVectorsPatchFieldLoader<>(
                // Recreate the object for each leaf so that different segments can be searched concurrently.
                DocValuesSyntheticFieldLoader::new,
                DocValuesSyntheticFieldLoader::copyVectorsAsList
            );
        }
        return null;
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
                if (values.docID() > docId) {
                    return hasValue = false;
                }
                return hasValue = values.docID() == docId || values.advance(docId) == docId;
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
            assert ref.length % fieldType().element.getNumBytes(fieldType().dims) == 0;
            int numVecs = ref.length / fieldType().element.getNumBytes(fieldType().dims);
            for (int i = 0; i < numVecs; i++) {
                b.startArray();
                int dims = fieldType().element.elementType() == DenseVectorFieldMapper.ElementType.BIT
                    ? fieldType().dims / Byte.SIZE
                    : fieldType().dims;
                for (int dim = 0; dim < dims; dim++) {
                    fieldType().element.readAndWriteValue(byteBuffer, b);
                }
                b.endArray();
            }
            b.endArray();
        }

        /**
         * Returns deep-copied vectors  for the current document, either as a list.
         *
         * @throws IOException if reading fails
         */
        private List<List<?>> copyVectorsAsList() throws IOException {
            assert hasValue : "rank vector is null";
            BytesRef ref = values.binaryValue();
            ByteBuffer byteBuffer = ByteBuffer.wrap(ref.bytes, ref.offset, ref.length).order(ByteOrder.LITTLE_ENDIAN);
            assert ref.length % fieldType().element.getNumBytes(fieldType().dims) == 0;
            int numVecs = ref.length / fieldType().element.getNumBytes(fieldType().dims);
            List<List<?>> vectors = new ArrayList<>(numVecs);
            for (int i = 0; i < numVecs; i++) {
                int dims = fieldType().element.elementType() == DenseVectorFieldMapper.ElementType.BIT
                    ? fieldType().dims / Byte.SIZE
                    : fieldType().dims;

                switch (fieldType().element.elementType()) {
                    case FLOAT -> {
                        List<Float> vec = new ArrayList<>(dims);
                        for (int dim = 0; dim < dims; dim++) {
                            vec.add(byteBuffer.getFloat());
                        }
                        vectors.add(vec);
                    }
                    case BYTE, BIT -> {
                        List<Byte> vec = new ArrayList<>(dims);
                        for (int dim = 0; dim < dims; dim++) {
                            vec.add(byteBuffer.get());
                        }
                        vectors.add(vec);
                    }
                }
            }
            return vectors;
        }

        @Override
        public String fieldName() {
            return fullPath();
        }
    }
}
