/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ilm;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * This class contains information about the current phase being executed by Index
 * Lifecycle Management on the specific index.
 */
public class PhaseExecutionInfo implements ToXContentObject {
    private static final ParseField POLICY_NAME_FIELD = new ParseField("policy");
    private static final ParseField PHASE_DEFINITION_FIELD = new ParseField("phase_definition");
    private static final ParseField VERSION_FIELD = new ParseField("version");
    private static final ParseField MODIFIED_DATE_IN_MILLIS_FIELD = new ParseField("modified_date_in_millis");

    private static final ConstructingObjectParser<PhaseExecutionInfo, String> PARSER = new ConstructingObjectParser<>(
        "phase_execution_info", true,
        (a, name) -> new PhaseExecutionInfo((String) a[0], (Phase) a[1], (long) a[2], (long) a[3]));
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), POLICY_NAME_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), Phase::parse, PHASE_DEFINITION_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), VERSION_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MODIFIED_DATE_IN_MILLIS_FIELD);
    }

    public static PhaseExecutionInfo parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    private final String policyName;
    private final Phase phase;
    private final long version;
    private final long modifiedDate;

    /**
     * This class holds information about the current phase that is being executed
     *
     * @param policyName the name of the policy being executed, this may not be the current policy assigned to an index
     * @param phase the current phase definition executed
     * @param version the version of the <code>policyName</code> being executed
     * @param modifiedDate the time the executing version of the phase was modified
     */
    public PhaseExecutionInfo(String policyName, Phase phase, long version, long modifiedDate) {
        this.policyName = policyName;
        this.phase = phase;
        this.version = version;
        this.modifiedDate = modifiedDate;
    }

    public String getPolicyName() {
        return policyName;
    }

    public Phase getPhase() {
        return phase;
    }

    public long getVersion() {
        return version;
    }

    public long getModifiedDate() {
        return modifiedDate;
    }

    @Override
    public int hashCode() {
        return Objects.hash(policyName, phase, version, modifiedDate);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PhaseExecutionInfo other = (PhaseExecutionInfo) obj;
        return Objects.equals(policyName, other.policyName) &&
            Objects.equals(phase, other.phase) &&
            Objects.equals(version, other.version) &&
            Objects.equals(modifiedDate, other.modifiedDate);
    }

    @Override
    public String toString() {
        return Strings.toString(this, false, true);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(POLICY_NAME_FIELD.getPreferredName(), policyName);
        if (phase != null) {
            builder.field(PHASE_DEFINITION_FIELD.getPreferredName(), phase);
        }
        builder.field(VERSION_FIELD.getPreferredName(), version);
        builder.timeField(MODIFIED_DATE_IN_MILLIS_FIELD.getPreferredName(), "modified_date", modifiedDate);
        builder.endObject();
        return builder;
    }
}

