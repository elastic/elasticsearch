/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.regex.Matcher;

import static org.elasticsearch.action.ValidateActions.addValidationError;
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
        groups = in.readOrderedMap(StreamInput::readString, (stream) -> {
            SingleGroupSource.Type groupType = SingleGroupSource.Type.fromId(stream.readByte());
            return switch (groupType) {
                case TERMS -> new TermsGroupSource(stream);
                case HISTOGRAM -> new HistogramGroupSource(stream);
                case DATE_HISTOGRAM -> new DateHistogramGroupSource(stream);
                case GEOTILE_GRID -> new GeoTileGroupSource(stream);
            };
        });
    }

    public Map<String, SingleGroupSource> getGroups() {
        return groups;
    }

    public Collection<String> getUsedNames() {
        return groups != null ? groups.keySet() : Collections.emptySet();
    }

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        if (groups == null) {
            validationException = addValidationError("pivot.groups must not be null", validationException);
        } else {
            for (SingleGroupSource group : groups.values()) {
                validationException = group.validate(validationException);
            }
        }
        return validationException;
    }

    public void checkForDeprecations(String id, NamedXContentRegistry namedXContentRegistry, Consumer<DeprecationIssue> onDeprecation) {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(source);
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
            try (
                XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(source);
                XContentParser sourceParser = XContentType.JSON.xContent()
                    .createParser(registry, LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(xContentBuilder).streamInput())
            ) {
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

    private static Map<String, SingleGroupSource> parseGroupConfig(final XContentParser parser, boolean lenient) throws IOException {
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

            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            String destinationFieldName = parser.currentName();
            if (validAggMatcher.reset(destinationFieldName).matches() == false) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Invalid group name [" + destinationFieldName + "]. Group names can contain any character except '[', ']', and '>'"
                );
            }

            token = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
            token = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            SingleGroupSource.Type groupType = SingleGroupSource.Type.valueOf(parser.currentName().toUpperCase(Locale.ROOT));

            token = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
            SingleGroupSource groupSource = switch (groupType) {
                case TERMS -> TermsGroupSource.fromXContent(parser, lenient);
                case HISTOGRAM -> HistogramGroupSource.fromXContent(parser, lenient);
                case DATE_HISTOGRAM -> DateHistogramGroupSource.fromXContent(parser, lenient);
                case GEOTILE_GRID -> GeoTileGroupSource.fromXContent(parser, lenient);
            };

            parser.nextToken();

            groups.put(destinationFieldName, groupSource);
        }
        return groups;
    }
}
