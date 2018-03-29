/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A {@link LifecycleAction} which deletes the index.
 */
public class RolloverAction implements LifecycleAction {
    public static final String NAME = "rollover";
    public static final ParseField ALIAS_FIELD = new ParseField("alias");
    public static final ParseField MAX_SIZE_FIELD = new ParseField("max_size");
    public static final ParseField MAX_DOCS_FIELD = new ParseField("max_docs");
    public static final ParseField MAX_AGE_FIELD = new ParseField("max_age");

    private static final Logger logger = ESLoggerFactory.getLogger(RolloverAction.class);
    private static final ConstructingObjectParser<RolloverAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a -> new RolloverAction((String) a[0], (ByteSizeValue) a[1], (TimeValue) a[2], (Long) a[3]));
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ALIAS_FIELD);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_SIZE_FIELD.getPreferredName()), MAX_SIZE_FIELD, ValueType.VALUE);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> TimeValue.parseTimeValue(p.text(), MAX_AGE_FIELD.getPreferredName()), MAX_AGE_FIELD, ValueType.VALUE);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), MAX_DOCS_FIELD);
    }

    private final String alias;
    private final ByteSizeValue maxSize;
    private final Long maxDocs;
    private final TimeValue maxAge;

    public static RolloverAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public RolloverAction(String alias, ByteSizeValue maxSize, TimeValue maxAge, Long maxDocs) {
        if (alias == null) {
            throw new IllegalArgumentException(ALIAS_FIELD.getPreferredName() + " must be not be null");
        }
        if (maxSize == null && maxAge == null && maxDocs == null) {
            throw new IllegalArgumentException("At least one rollover condition must be set.");
        }
        this.alias = alias;
        this.maxSize = maxSize;
        this.maxAge = maxAge;
        this.maxDocs = maxDocs;
    }

    public RolloverAction(StreamInput in) throws IOException {
        alias = in.readString();
        if (in.readBoolean()) {
            maxSize = new ByteSizeValue(in);
        } else {
            maxSize = null;
        }
        if (in.readBoolean()) {
            maxAge = new TimeValue(in);
        } else {
            maxAge = null;
        }
        if (in.readBoolean()) {
            maxDocs = in.readVLong();
        } else {
            maxDocs = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(alias);
        boolean hasMaxSize = maxSize != null;
        out.writeBoolean(hasMaxSize);
        if (hasMaxSize) {
            maxSize.writeTo(out);
        }
        boolean hasMaxAge = maxAge != null;
        out.writeBoolean(hasMaxAge);
        if (hasMaxAge) {
            maxAge.writeTo(out);
        }
        boolean hasMaxDocs = maxDocs != null;
        out.writeBoolean(hasMaxDocs);
        if (hasMaxDocs) {
            out.writeVLong(maxDocs);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public String getAlias() {
        return alias;
    }

    public ByteSizeValue getMaxSize() {
        return maxSize;
    }

    public TimeValue getMaxAge() {
        return maxAge;
    }
    
    public Long getMaxDocs() {
        return maxDocs;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ALIAS_FIELD.getPreferredName(), alias);
        if (maxSize != null) {
            builder.field(MAX_SIZE_FIELD.getPreferredName(), maxSize.getStringRep());
        }
        if (maxAge != null) {
            builder.field(MAX_AGE_FIELD.getPreferredName(), maxAge.getStringRep());
        }
        if (maxDocs != null) {
            builder.field(MAX_DOCS_FIELD.getPreferredName(), maxDocs);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, Step.StepKey nextStepKey) {
        return Collections.emptyList();
//        ConditionalWaitStep wait = new ConditionalWaitStep(clusterService, "wait_for_rollover", index.getName(), phase, action, (clusterState) -> {
//            // TODO(talevy): actually, needs to RolloverRequest with dryrun to get the appropriate data; clusterState is not enough...
//            // can potentially reduce down to original problem with RolloverRequest...1minute...RolloverRequest...1minute... probably ok?
//            if (clusterService.state().getMetaData().index(index.getName()).getAliases().containsKey(alias)) {
//                RolloverRequest rolloverRequest = new RolloverRequest(alias, null);
//                if (maxAge != null) {
//                    rolloverRequest.addMaxIndexAgeCondition(maxAge);
//                }
//                if (maxSize != null) {
//                    rolloverRequest.addMaxIndexSizeCondition(maxSize);
//                }
//                if (maxDocs != null) {
//                    rolloverRequest.addMaxIndexDocsCondition(maxDocs);
//                }
//                client.admin().indices().rolloverIndex(rolloverRequest, new ActionListener<RolloverResponse>() {
//                    @Override
//                    public void onResponse(RolloverResponse rolloverResponse) {
//                        return rolloverResponse.isRolledOver();
//                    }
//
//                    @Override
//                    public void onFailure(Exception e) {
//                        listener.onFailure(e);
//                    }
//                });
//            } else {
//                listener.onSuccess(true);
//            }
//        });

    }

    @Override
    public int hashCode() {
        return Objects.hash(alias, maxSize, maxAge, maxDocs);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        RolloverAction other = (RolloverAction) obj;
        return Objects.equals(alias, other.alias) &&
                Objects.equals(maxSize, other.maxSize) &&
                Objects.equals(maxAge, other.maxAge) &&
                Objects.equals(maxDocs, other.maxDocs);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
