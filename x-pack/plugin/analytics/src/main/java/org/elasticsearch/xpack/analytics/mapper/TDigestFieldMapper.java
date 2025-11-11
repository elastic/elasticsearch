/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexHistogramFieldData;
import org.elasticsearch.index.fielddata.LeafHistogramFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.CompositeSyntheticFieldLoader;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IgnoreMalformedStoredValues;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.CopyingXContentParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentSubParser;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Field Mapper for pre-aggregated histograms.
 */
public class TDigestFieldMapper extends FieldMapper {
    public static final FeatureFlag TDIGEST_FIELD_MAPPER = new FeatureFlag("tdigest_field_mapper");

    public static final String CENTROIDS_NAME = "centroids";
    public static final String COUNTS_NAME = "counts";
    public static final String CONTENT_TYPE = "tdigest";

    private static TDigestFieldMapper toType(FieldMapper in) {
        return (TDigestFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {
        private static final int DEFAULT_COMPRESSION = 100;
        private static final int MAXIMUM_COMPRESSION = 10000;

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<Explicit<Boolean>> ignoreMalformed;
        private final Parameter<TDigestState.Type> digestType;
        private final Parameter<Integer> compression;

        public Builder(String name, boolean ignoreMalformedByDefault) {
            super(name);
            this.ignoreMalformed = Parameter.explicitBoolParam(
                "ignore_malformed",
                true,
                m -> toType(m).ignoreMalformed,
                ignoreMalformedByDefault
            );
            this.digestType = Parameter.enumParam(
                "digest_type",
                false,
                m -> toType(m).digestType,
                TDigestState.Type.HYBRID,
                TDigestState.Type.class
            );
            this.compression = Parameter.intParam("compression", false, m -> toType(m).compression, DEFAULT_COMPRESSION).addValidator(c -> {
                if (c <= 0 || c > MAXIMUM_COMPRESSION) {
                    throw new IllegalArgumentException(
                        "compression must be a positive integer between 1 and " + MAXIMUM_COMPRESSION + " was [" + c + "]"
                    );
                }
            });
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { digestType, compression, ignoreMalformed, meta };
        }

        @Override
        public TDigestFieldMapper build(MapperBuilderContext context) {
            return new TDigestFieldMapper(
                leafName(),
                new TDigestFieldType(context.buildFullName(leafName()), meta.getValue()),
                builderParams(this, context),
                this
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser(
        (n, c) -> new Builder(n, IGNORE_MALFORMED_SETTING.get(c.getSettings())),
        notInMultiFields(CONTENT_TYPE)
    );

    private final Explicit<Boolean> ignoreMalformed;
    private final boolean ignoreMalformedByDefault;
    private final TDigestState.Type digestType;
    private final int compression;

    public TDigestFieldMapper(String simpleName, MappedFieldType mappedFieldType, BuilderParams builderParams, Builder builder) {
        super(simpleName, mappedFieldType, builderParams);
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.ignoreMalformedByDefault = builder.ignoreMalformed.getDefaultValue().value();
        this.digestType = builder.digestType.getValue();
        this.compression = builder.compression.getValue();
    }

    @Override
    public boolean ignoreMalformed() {
        return ignoreMalformed.value();
    }

    public TDigestState.Type digestType() {
        return digestType;
    }

    public int compression() {
        return compression;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), ignoreMalformedByDefault).init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    public static class TDigestFieldType extends MappedFieldType {

        public TDigestFieldType(String name, Map<String, String> meta) {
            super(name, IndexType.docValuesOnly(), false, meta);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public boolean isSearchable() {
            return false;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            // TODO - This needs to be changed to a custom values source type
            return (cache, breakerService) -> new IndexHistogramFieldData(name(), AnalyticsValuesSourceType.HISTOGRAM) {

                @Override
                public LeafHistogramFieldData load(LeafReaderContext context) {
                    return new LeafHistogramFieldData() {
                        @Override
                        public HistogramValues getHistogramValues() throws IOException {
                            try {
                                final BinaryDocValues values = DocValues.getBinary(context.reader(), fieldName);
                                final InternalHistogramValue value = new InternalHistogramValue();
                                return new HistogramValues() {

                                    @Override
                                    public boolean advanceExact(int doc) throws IOException {
                                        return values.advanceExact(doc);
                                    }

                                    @Override
                                    public HistogramValue histogram() throws IOException {
                                        try {
                                            value.reset(values.binaryValue());
                                            return value;
                                        } catch (IOException e) {
                                            throw new IOException("Cannot load doc value", e);
                                        }
                                    }
                                };
                            } catch (IOException e) {
                                throw new IOException("Cannot load doc values", e);
                            }
                        }

                        @Override
                        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
                            throw new UnsupportedOperationException("The [" + CONTENT_TYPE + "] field does not " + "support scripts");
                        }

                        @Override
                        public FormattedDocValues getFormattedValues(DocValueFormat format) {
                            try {
                                final BinaryDocValues values = DocValues.getBinary(context.reader(), fieldName);
                                final InternalHistogramValue value = new InternalHistogramValue();
                                return new FormattedDocValues() {
                                    @Override
                                    public boolean advanceExact(int docId) throws IOException {
                                        return values.advanceExact(docId);
                                    }

                                    @Override
                                    public int docValueCount() {
                                        return 1;
                                    }

                                    @Override
                                    public Object nextValue() throws IOException {
                                        value.reset(values.binaryValue());
                                        return value;
                                    }
                                };
                            } catch (IOException e) {
                                throw new UncheckedIOException("Unable to loead histogram doc values", e);
                            }
                        }

                        @Override
                        public SortedBinaryDocValues getBytesValues() {
                            throw new UnsupportedOperationException(
                                "String representation of doc values " + "for [" + CONTENT_TYPE + "] fields is not supported"
                            );
                        }

                        @Override
                        public long ramBytesUsed() {
                            return 0; // Unknown
                        }

                    };
                }

                @Override
                public LeafHistogramFieldData loadDirect(LeafReaderContext context) {
                    return load(context);
                }

                @Override
                public SortField sortField(Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
                    throw new IllegalArgumentException("can't sort on the [" + CONTENT_TYPE + "] field");
                }

                @Override
                public BucketedSort newBucketedSort(
                    BigArrays bigArrays,
                    Object missingValue,
                    MultiValueMode sortMode,
                    Nested nested,
                    SortOrder sortOrder,
                    DocValueFormat format,
                    int bucketSize,
                    BucketedSort.ExtraData extra
                ) {
                    throw new IllegalArgumentException("can't sort on the [" + CONTENT_TYPE + "] field");
                }
            };
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException(
                "[" + CONTENT_TYPE + "] field do not support searching, " + "use dedicated aggregations instead: [" + name() + "]"
            );
        }
    }

    @Override
    protected boolean supportsParsingObject() {
        return true;
    }

    @Override
    public void parse(DocumentParserContext context) throws IOException {
        context.path().add(leafName());

        boolean shouldStoreMalformedDataForSyntheticSource = context.mappingLookup().isSourceSynthetic() && ignoreMalformed();
        XContentParser.Token token;
        XContentSubParser subParser = null;
        XContentBuilder malformedDataForSyntheticSource = null;

        try {
            token = context.parser().currentToken();
            if (token == XContentParser.Token.VALUE_NULL) {
                context.path().remove();
                return;
            }
            // should be an object
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, context.parser());
            if (shouldStoreMalformedDataForSyntheticSource) {
                var copyingParser = new CopyingXContentParser(context.parser());
                malformedDataForSyntheticSource = copyingParser.getBuilder();
                subParser = new XContentSubParser(copyingParser);
            } else {
                subParser = new XContentSubParser(context.parser());
            }
            subParser.nextToken();
            // TODO: Here we should build a t-digest out of the input, based on the settings on the field
            TDigestParser.ParsedHistogram parsedHistogram = TDigestParser.parse(fullPath(), subParser);

            BytesStreamOutput streamOutput = new BytesStreamOutput();
            for (int i = 0; i < parsedHistogram.centroids().size(); i++) {
                long count = parsedHistogram.counts().get(i);
                assert count >= 0;
                // we do not add elements with count == 0
                if (count > 0) {
                    streamOutput.writeVLong(count);
                    streamOutput.writeLong(Double.doubleToRawLongBits(parsedHistogram.centroids().get(i)));
                }
            }
            BytesRef docValue = streamOutput.bytes().toBytesRef();
            Field field = new BinaryDocValuesField(fullPath(), docValue);
            if (context.doc().getByKey(fieldType().name()) != null) {
                throw new IllegalArgumentException(
                    "Field ["
                        + fullPath()
                        + "] of type ["
                        + typeName()
                        + "] doesn't support indexing multiple values for the same field in the same document"
                );
            }
            context.doc().addWithKey(fieldType().name(), field);

        } catch (Exception ex) {
            if (ignoreMalformed.value() == false) {
                throw new DocumentParsingException(
                    context.parser().getTokenLocation(),
                    "failed to parse field [" + fieldType().name() + "] of type [" + fieldType().typeName() + "]",
                    ex
                );
            }

            if (subParser != null) {
                // close the subParser so we advance to the end of the object
                subParser.close();
            } else if (shouldStoreMalformedDataForSyntheticSource) {
                // We have a malformed value, but it's not an object given that `subParser` is null.
                // So we just remember whatever it is.
                malformedDataForSyntheticSource = XContentBuilder.builder(context.parser().contentType().xContent())
                    .copyCurrentStructure(context.parser());
            }

            if (malformedDataForSyntheticSource != null) {
                context.doc().add(IgnoreMalformedStoredValues.storedField(fullPath(), malformedDataForSyntheticSource));
            }

            context.addIgnoredField(fieldType().name());
        }
        context.path().remove();
    }

    /** re-usable {@link HistogramValue} implementation */
    private static class InternalHistogramValue extends HistogramValue {
        double value;
        long count;
        boolean isExhausted;
        final ByteArrayStreamInput streamInput;

        InternalHistogramValue() {
            streamInput = new ByteArrayStreamInput();
        }

        /** reset the value for the histogram */
        void reset(BytesRef bytesRef) {
            streamInput.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            isExhausted = false;
            value = 0;
            count = 0;
        }

        @Override
        public boolean next() throws IOException {
            if (streamInput.available() > 0) {
                if (streamInput.getTransportVersion().onOrAfter(TransportVersions.V_8_11_X)) {
                    count = streamInput.readVLong();
                } else {
                    count = streamInput.readVInt();
                }
                value = Double.longBitsToDouble(streamInput.readLong());
                return true;
            }
            isExhausted = true;
            return false;
        }

        @Override
        public double value() {
            if (isExhausted) {
                throw new IllegalArgumentException("histogram already exhausted");
            }
            return value;
        }

        @Override
        public long count() {
            if (isExhausted) {
                throw new IllegalArgumentException("histogram already exhausted");
            }
            return count;
        }
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport.Native(
            () -> new CompositeSyntheticFieldLoader(
                leafName(),
                fullPath(),
                new HistogramSyntheticFieldLoader(),
                new CompositeSyntheticFieldLoader.MalformedValuesLayer(fullPath())
            )
        );
    }

    private class HistogramSyntheticFieldLoader implements CompositeSyntheticFieldLoader.DocValuesLayer {
        private final InternalHistogramValue value = new InternalHistogramValue();
        private BytesRef binaryValue;

        @Override
        public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
            BinaryDocValues docValues = leafReader.getBinaryDocValues(fieldType().name());
            if (docValues == null) {
                // No values in this leaf
                binaryValue = null;
                return null;
            }
            return docId -> {
                if (docValues.advanceExact(docId)) {
                    binaryValue = docValues.binaryValue();
                    return true;
                }
                binaryValue = null;
                return false;
            };
        }

        @Override
        public boolean hasValue() {
            return binaryValue != null;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            if (binaryValue == null) {
                return;
            }
            b.startObject();

            value.reset(binaryValue);
            b.startArray(CENTROIDS_NAME);
            while (value.next()) {
                b.value(value.value());
            }
            b.endArray();

            value.reset(binaryValue);
            b.startArray(COUNTS_NAME);
            while (value.next()) {
                b.value(value.count());
            }
            b.endArray();

            b.endObject();
        }

        @Override
        public String fieldName() {
            return fullPath();
        }

        @Override
        public long valueCount() {
            return binaryValue != null ? 1 : 0;
        }
    };
}
