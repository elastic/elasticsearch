/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/*
 * Wraps a single group for groupby
 */
public class GroupConfig implements Writeable, ToXContentObject {

    private static final Logger logger = LogManager.getLogger(GroupConfig.class);

    private final Map<String, Object> source;
    private final Map<String, SingleGroupSource> groups;

    public GroupConfig(final Map<String, Object> source, final Map<String, SingleGroupSource> groups) {
        this.source = ExceptionsHelper.requireNonNull(source, TransformField.GROUP_BY.getPreferredName());
        this.groups = groups;
    }

    public GroupConfig(StreamInput in) throws IOException {
        source = in.readMap();
        groups = in.readMap(StreamInput::readString, (stream) -> {
            SingleGroupSource.Type groupType = SingleGroupSource.Type.fromId(stream.readByte());
            switch (groupType) {
            case TERMS:
                return new TermsGroupSource(stream);
            case HISTOGRAM:
                return new HistogramGroupSource(stream);
            case DATE_HISTOGRAM:
                return new DateHistogramGroupSource(stream);
            default:
                throw new IOException("Unknown group type");
            }
        });
    }

    public Map <String, SingleGroupSource> getGroups() {
        return groups;
    }

    public boolean isValid() {
        return this.groups != null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(source);
        out.writeMap(groups, StreamOutput::writeString, (stream, value) -> {
            stream.writeByte(value.getType().getId());
            value.writeTo(stream);
        });
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.map(source);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final GroupConfig that = (GroupConfig) other;

        return Objects.equals(this.source, that.source) && Objects.equals(this.groups, that.groups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, groups);
    }

    public static GroupConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        NamedXContentRegistry registry = parser.getXContentRegistry();
        Map<String, Object> source = parser.mapOrdered();
        Map<String, SingleGroupSource> groups = null;

        if (source.isEmpty()) {
            if (lenient) {
                logger.warn(TransformMessages.TRANSFORM_CONFIGURATION_PIVOT_NO_GROUP_BY);
            } else {
                throw new IllegalArgumentException(TransformMessages.TRANSFORM_CONFIGURATION_PIVOT_NO_GROUP_BY);
            }
        } else {
            try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(source);
                    XContentParser sourceParser = XContentType.JSON.xContent().createParser(registry, LoggingDeprecationHandler.INSTANCE,
                            BytesReference.bytes(xContentBuilder).streamInput())) {
                groups = parseGroupConfig(sourceParser, lenient);
            } catch (Exception e) {
                if (lenient) {
                    logger.warn(TransformMessages.LOG_TRANSFORM_CONFIGURATION_BAD_GROUP_BY, e);
                } else {
                    throw e;
                }
            }
        }
        return new GroupConfig(source, groups);
    }

    private static Map<String, SingleGroupSource> parseGroupConfig(final XContentParser parser,
            boolean lenient) throws IOException {
        Matcher validAggMatcher = AggregatorFactories.VALID_AGG_NAME.matcher("");
        LinkedHashMap<String, SingleGroupSource> groups = new LinkedHashMap<>();

        // be parsing friendly, whether the token needs to be advanced or not (similar to what ObjectParser does)
        XContentParser.Token token;
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "Failed to parse object: Expected START_OBJECT but was: " + token);
            }
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {

            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
            String destinationFieldName = parser.currentName();
            if (validAggMatcher.reset(destinationFieldName).matches() == false) {
                throw new ParsingException(parser.getTokenLocation(), "Invalid group name [" + destinationFieldName
                        + "]. Group names can contain any character except '[', ']', and '>'");
            }

            token = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
            token = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
            SingleGroupSource.Type groupType = SingleGroupSource.Type.valueOf(parser.currentName().toUpperCase(Locale.ROOT));

            token = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
            SingleGroupSource groupSource;
            switch (groupType) {
            case TERMS:
                groupSource = TermsGroupSource.fromXContent(parser, lenient);
                break;
            case HISTOGRAM:
                groupSource = HistogramGroupSource.fromXContent(parser, lenient);
                break;
            case DATE_HISTOGRAM:
                groupSource = DateHistogramGroupSource.fromXContent(parser, lenient);
                break;
            default:
                throw new ParsingException(parser.getTokenLocation(), "invalid grouping type: " + groupType);
            }

            parser.nextToken();

            groups.put(destinationFieldName, groupSource);
        }
        return groups;
    }
}
