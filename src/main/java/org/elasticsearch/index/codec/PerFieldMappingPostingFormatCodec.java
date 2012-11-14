package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40Codec;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.mapper.MapperService;

/**
 * This one is the "default" codec we use.
 */
// LUCENE UPGRADE: make sure to move to a new codec depending on the lucene version
public class PerFieldMappingPostingFormatCodec extends Lucene40Codec {

    private final MapperService mapperService;
    private final PostingsFormat defaultPostingFormat;

    public PerFieldMappingPostingFormatCodec(MapperService mapperService, PostingsFormat defaultPostingFormat) {
        this.mapperService = mapperService;
        this.defaultPostingFormat = defaultPostingFormat;
    }

    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
        PostingsFormatProvider postingsFormat = mapperService.indexName(field).mapper().postingFormatProvider();
        return postingsFormat != null ? postingsFormat.get() : defaultPostingFormat;
    }
}
