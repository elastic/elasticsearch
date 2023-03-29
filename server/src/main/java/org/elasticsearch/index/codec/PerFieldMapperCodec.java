/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.bloomfilter.ES87BloomFilterPostingsFormat;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;
import java.util.PriorityQueue;

/**
 * {@link PerFieldMapperCodec This Lucene codec} provides the default
 * {@link PostingsFormat} and {@link KnnVectorsFormat} for Elasticsearch. It utilizes the
 * {@link MapperService} to lookup a {@link PostingsFormat} and {@link KnnVectorsFormat} per field. This
 * allows users to change the low level postings format and vectors format for individual fields
 * per index in real time via the mapping API. If no specific postings format or vector format is
 * configured for a specific field the default postings or vector format is used.
 */
public class PerFieldMapperCodec extends Codec {

    // temporarily wrap Lucene95Code because we cannot extend it
    private final Lucene95Codec l95;

    // overwrite compoundFormat with modification of Lucene90CompoundFormat()
    // that has its own "write" implementation that takes file sizes into account

    private final MapperService mapperService;
    private final DocValuesFormat docValuesFormat = new Lucene90DocValuesFormat();
    private final ES87BloomFilterPostingsFormat bloomFilterPostingsFormat;
    private final ES87TSDBDocValuesFormat tsdbDocValuesFormat;

    // static {
    // assert Codec.forName(Lucene.LATEST_CODEC).getClass().isAssignableFrom(PerFieldMapperCodec.class)
    // : "PerFieldMapperCodec must subclass the latest lucene codec: " + Lucene.LATEST_CODEC;
    // }

    private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
        public PostingsFormat getPostingsFormatForField(String field) {
            return PerFieldMapperCodec.this.getPostingsFormatForField(field);
        }
    };

    public PerFieldMapperCodec(Lucene95Codec.Mode compressionMode, MapperService mapperService, BigArrays bigArrays) {
        super("Lucene95");
        this.l95 = new Lucene95Codec(compressionMode);
        this.mapperService = mapperService;
        this.bloomFilterPostingsFormat = new ES87BloomFilterPostingsFormat(bigArrays, this::internalGetPostingsFormatForField);
        this.tsdbDocValuesFormat = new ES87TSDBDocValuesFormat();
    }

    public PostingsFormat getPostingsFormatForField(String field) {
        if (useBloomFilter(field)) {
            return bloomFilterPostingsFormat;
        }
        return internalGetPostingsFormatForField(field);
    }

    private PostingsFormat internalGetPostingsFormatForField(String field) {
        final PostingsFormat format = mapperService.mappingLookup().getPostingsFormat(field);
        if (format != null) {
            return format;
        }
        return l95.getPostingsFormatForField(field);
    }

    boolean useBloomFilter(String field) {
        IndexSettings indexSettings = mapperService.getIndexSettings();
        if (mapperService.mappingLookup().isDataStreamTimestampFieldEnabled()) {
            // In case for time series indices, they _id isn't randomly generated,
            // but based on dimension fields and timestamp field, so during indexing
            // version/seq_no/term needs to be looked up and having a bloom filter
            // can speed this up significantly.
            return indexSettings.getMode() == IndexMode.TIME_SERIES
                && IdFieldMapper.NAME.equals(field)
                && IndexSettings.BLOOM_FILTER_ID_FIELD_ENABLED_SETTING.get(indexSettings.getSettings());
        } else {
            return IdFieldMapper.NAME.equals(field) && IndexSettings.BLOOM_FILTER_ID_FIELD_ENABLED_SETTING.get(indexSettings.getSettings());
        }
    }

    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        Mapper mapper = mapperService.mappingLookup().getMapper(field);
        if (mapper instanceof DenseVectorFieldMapper vectorMapper) {
            KnnVectorsFormat format = vectorMapper.getKnnVectorsFormatForField();
            if (format != null) {
                return format;
            }
        }
        return l95.getKnnVectorsFormatForField(field);
    }

    public DocValuesFormat getDocValuesFormatForField(String field) {
        if (useTSDBDocValuesFormat(field)) {
            return tsdbDocValuesFormat;
        }
        return docValuesFormat;
    }

    boolean useTSDBDocValuesFormat(final String field) {
        return mapperService.getIndexSettings().isES87TSDBCodecEnabled()
            && isTimeSeriesModeIndex()
            && isNotSpecialField(field)
            && (isCounterOrGaugeMetricType(field) || isTimestampField(field));
    }

    private boolean isTimeSeriesModeIndex() {
        return IndexMode.TIME_SERIES.equals(mapperService.getIndexSettings().getMode());
    }

    private boolean isCounterOrGaugeMetricType(String field) {
        if (mapperService != null) {
            final MappingLookup mappingLookup = mapperService.mappingLookup();
            if (mappingLookup.getMapper(field) instanceof NumberFieldMapper) {
                final MappedFieldType fieldType = mappingLookup.getFieldType(field);
                return TimeSeriesParams.MetricType.COUNTER.equals(fieldType.getMetricType())
                    || TimeSeriesParams.MetricType.GAUGE.equals(fieldType.getMetricType());
            }
        }
        return false;
    }

    private boolean isTimestampField(String field) {
        return "@timestamp".equals(field);
    }

    private boolean isNotSpecialField(String field) {
        return field.startsWith("_") == false;
    }

    @Override
    public PostingsFormat postingsFormat() {
        return this.postingsFormat;
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return l95.docValuesFormat();
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return l95.storedFieldsFormat();
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
        return l95.termVectorsFormat();
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return l95.fieldInfosFormat();
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return l95.segmentInfoFormat();
    }

    @Override
    public NormsFormat normsFormat() {
        return l95.normsFormat();
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
        return l95.liveDocsFormat();
    }

    @Override
    public PointsFormat pointsFormat() {
        return l95.pointsFormat();
    }

    @Override
    public KnnVectorsFormat knnVectorsFormat() {
        return l95.knnVectorsFormat();
    }

    @Override
    public CompoundFormat compoundFormat() {
        return new CompoundFormat() {
            static final String DATA_EXTENSION = "cfs";
            /** Extension of compound file entries */
            static final String ENTRIES_EXTENSION = "cfe";

            static final String DATA_CODEC = "Lucene90CompoundData";
            static final String ENTRY_CODEC = "Lucene90CompoundEntries";
            static final int VERSION_START = 0;
            static final int VERSION_CURRENT = VERSION_START;

            @Override
            public CompoundDirectory getCompoundReader(Directory dir, SegmentInfo si, IOContext context) throws IOException {
                return l95.compoundFormat().getCompoundReader(dir, si, context);
            }

            public void write(Directory dir, SegmentInfo si, IOContext context) throws IOException {
                String dataFile = IndexFileNames.segmentFileName(si.name, "", DATA_EXTENSION);
                String entriesFile = IndexFileNames.segmentFileName(si.name, "", ENTRIES_EXTENSION);

                try (IndexOutput data = dir.createOutput(dataFile, context); IndexOutput entries = dir.createOutput(entriesFile, context)) {
                    CodecUtil.writeIndexHeader(data, DATA_CODEC, VERSION_CURRENT, si.getId(), "");
                    CodecUtil.writeIndexHeader(entries, ENTRY_CODEC, VERSION_CURRENT, si.getId(), "");

                    writeCompoundFile(entries, data, dir, si);

                    CodecUtil.writeFooter(data);
                    CodecUtil.writeFooter(entries);
                }
            }

            record FileWithLength(String filename, long length) {};

            private void writeCompoundFile(IndexOutput entries, IndexOutput data, Directory dir, SegmentInfo si) throws IOException {
                // write number of files
                int numFiles = si.files().size();
                entries.writeVInt(numFiles);

                // first put files in ascending size order
                PriorityQueue<FileWithLength> pq = new PriorityQueue<>(numFiles, (o1, o2) -> Long.compare(o1.length, o2.length));
                for (String file : si.files()) {
                    pq.add(new FileWithLength(file, dir.fileLength(file)));
                }
                while (pq.isEmpty() == false) {
                    FileWithLength sizedFile = pq.poll();
                    String file = sizedFile.filename;
                    // align file start offset
                    long startOffset = data.alignFilePointer(Long.BYTES);
                    // write bytes for file
                    try (ChecksumIndexInput in = dir.openChecksumInput(file, IOContext.READONCE)) {

                        // just copies the index header, verifying that its id matches what we expect
                        CodecUtil.verifyAndCopyIndexHeader(in, data, si.getId());

                        // copy all bytes except the footer
                        long numBytesToCopy = in.length() - CodecUtil.footerLength() - in.getFilePointer();
                        data.copyBytes(in, numBytesToCopy);

                        // verify footer (checksum) matches for the incoming file we are copying
                        long checksum = CodecUtil.checkFooter(in);

                        // this is poached from CodecUtil.writeFooter, but we need to use our own checksum, not
                        // data.getChecksum(), but I think
                        // adding a public method to CodecUtil to do that is somewhat dangerous:
                        CodecUtil.writeBEInt(data, CodecUtil.FOOTER_MAGIC);
                        CodecUtil.writeBEInt(data, 0);
                        CodecUtil.writeBELong(data, checksum);
                    }
                    long endOffset = data.getFilePointer();

                    long length = endOffset - startOffset;

                    // write entry for file
                    entries.writeString(IndexFileNames.stripSegmentName(file));
                    entries.writeLong(startOffset);
                    entries.writeLong(length);
                }
            }
        };
    }
}
