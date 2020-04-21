package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PointValues;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.Rounding.Prepared;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;

public interface RoundingPreparer {
    public static final String NAME = "RoundingPreparer";

    Rounding.Prepared prepare(MappedFieldType ft, IndexReader reader, Rounding rounding) throws IOException;

    public class ForDate implements RoundingPreparer {
        @Override
        public Prepared prepare(MappedFieldType ft, IndexReader reader, Rounding rounding) throws IOException {
            if (ft.indexOptions() == IndexOptions.NONE) {
                return rounding.prepareForUnknown();
            }
            DateFieldType dft = (DateFieldType) ft;
            long minUtcMillis = dft.resolution().parsePointAsMillis(PointValues.getMinPackedValue(reader, ft.name()));
            long maxUtcMillis = dft.resolution().parsePointAsMillis(PointValues.getMinPackedValue(reader, ft.name()));
            return rounding.prepare(minUtcMillis, maxUtcMillis);
        }
    }
}
