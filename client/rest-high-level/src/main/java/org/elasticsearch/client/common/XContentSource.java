package org.elasticsearch.client.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.XContentUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates the xcontent source
 */
public class XContentSource {

    private final Object data;

    /**
     * Constructs a new XContentSource out of the given parser
     */
    public XContentSource(XContentParser parser) throws IOException {
        this.data = XContentUtils.readValue(parser, parser.nextToken());
    }

    /**
     * @return true if the top level value of the source is a map
     */
    public boolean isMap() {
        return data instanceof Map;
    }

    /**
     * @return The source as a map
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getAsMap() {
        return (Map<String, Object>) data;
    }

    /**
     * @return true if the top level value of the source is a list
     */
    public boolean isList() {
        return data instanceof List;
    }

    /**
     * @return The source as a list
     */
    @SuppressWarnings("unchecked")
    public List<Object> getAsList() {
        return (List<Object>) data;
    }

    /**
     * Extracts a value identified by the given path in the source.
     *
     * @param path a dot notation path to the requested value
     * @return The extracted value or {@code null} if no value is associated with the given path
     */
    @SuppressWarnings("unchecked")
    public <T> T getValue(String path) {
        return (T) ObjectPath.eval(path, data);
    }

}
