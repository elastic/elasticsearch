/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Information about deprecated items
 */
public class DeprecationIssue implements Writeable, ToXContentObject {

    private static final String ACTIONS_META_FIELD = "actions";
    private static final String OBJECTS_FIELD = "objects";
    private static final String ACTION_TYPE = "action_type";
    private static final String REMOVE_SETTINGS_ACTION_TYPE = "remove_settings";

    public enum Level implements Writeable {
        /**
         * Resolving this issue is advised but not required to upgrade. There may be undesired changes in behavior unless this issue is
         * resolved before upgrading.
         */
        WARNING,
        /**
         * This issue must be resolved to upgrade. Failures will occur unless this is resolved before upgrading.
         */
        CRITICAL;

        public static Level fromString(String value) {
            return Level.valueOf(value.toUpperCase(Locale.ROOT));
        }

        public static Level readFromStream(StreamInput in) throws IOException {
            int ordinal = in.readVInt();
            if (ordinal < 0 || ordinal >= values().length) {
                throw new IOException("Unknown Level ordinal [" + ordinal + "]");
            }
            return values()[ordinal];
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(ordinal());
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private final Level level;
    private final String message;
    private final String url;
    private final String details;
    private final boolean resolveDuringRollingUpgrade;
    private final Map<String, Object> meta;

    public DeprecationIssue(
        Level level,
        String message,
        String url,
        @Nullable String details,
        boolean resolveDuringRollingUpgrade,
        @Nullable Map<String, Object> meta
    ) {
        this.level = level;
        this.message = message;
        this.url = url;
        this.details = details;
        this.resolveDuringRollingUpgrade = resolveDuringRollingUpgrade;
        this.meta = meta;
    }

    public DeprecationIssue(StreamInput in) throws IOException {
        level = Level.readFromStream(in);
        message = in.readString();
        url = in.readString();
        details = in.readOptionalString();
        resolveDuringRollingUpgrade = in.getVersion().onOrAfter(Version.V_7_15_0) && in.readBoolean();
        meta = in.getVersion().onOrAfter(Version.V_7_14_0) ? in.readMap() : null;
    }

    public Level getLevel() {
        return level;
    }

    public String getMessage() {
        return message;
    }

    public String getUrl() {
        return url;
    }

    public String getDetails() {
        return details;
    }

    /**
     * @return whether a deprecation issue can only be resolved during a rolling upgrade when a node is offline.
     */
    public boolean isResolveDuringRollingUpgrade() {
        return resolveDuringRollingUpgrade;
    }

    /**
     * @return custom metadata, which allows the ui to display additional details
     *         without parsing the deprecation message itself.
     */
    public Map<String, Object> getMeta() {
        return meta;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        level.writeTo(out);
        out.writeString(message);
        out.writeString(url);
        out.writeOptionalString(details);
        if (out.getVersion().onOrAfter(Version.V_7_15_0)) {
            out.writeBoolean(resolveDuringRollingUpgrade);
        }
        if (out.getVersion().onOrAfter(Version.V_7_14_0)) {
            out.writeMap(meta);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("level", level).field("message", message).field("url", url);
        if (details != null) {
            builder.field("details", details);
        }
        builder.field("resolve_during_rolling_upgrade", resolveDuringRollingUpgrade);
        if (meta != null) {
            builder.field("_meta", meta);
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeprecationIssue that = (DeprecationIssue) o;
        return Objects.equals(level, that.level)
            && Objects.equals(message, that.message)
            && Objects.equals(url, that.url)
            && Objects.equals(details, that.details)
            && Objects.equals(resolveDuringRollingUpgrade, that.resolveDuringRollingUpgrade)
            && Objects.equals(meta, that.meta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(level, message, url, details, resolveDuringRollingUpgrade, meta);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static Map<String, Object> createMetaMapForRemovableSettings(List<String> removableSettings) {
        Map<String, Object> actionsMap = new HashMap<>();
        actionsMap.put(ACTION_TYPE, REMOVE_SETTINGS_ACTION_TYPE);
        actionsMap.put(OBJECTS_FIELD, removableSettings);
        return Collections.singletonMap(ACTIONS_META_FIELD, Collections.singletonList(Collections.unmodifiableMap(actionsMap)));
    }

    /**
     * This method returns a DeprecationIssue that has in its meta object the intersection of all auto-removable settings that appear on
     * all of the DeprecationIssues that are passed in. This method assumes that all DeprecationIssues passed in are equal, except for the
     * auto-removable settings in the meta object.
     * @param similarIssues
     * @return
     */
    @SuppressWarnings("unchecked")
    public static DeprecationIssue getIntersectionOfRemovableSettings(List<DeprecationIssue> similarIssues) {
        if (similarIssues == null || similarIssues.isEmpty()) {
            return null;
        }
        if (similarIssues.size() == 1) {
            return similarIssues.get(0);
        }
        boolean hasDeletableSettings = false;
        List<String> leastCommonRemovableSettings = null;
        for (DeprecationIssue issue : similarIssues) {
            Map<String, Object> meta = issue.getMeta();
            if (meta == null) {
                leastCommonRemovableSettings = null;
                break;
            }
            Object actionsObject = meta.get(ACTIONS_META_FIELD);
            if (actionsObject == null) {
                leastCommonRemovableSettings = null;
                break;
            }
            List<Map<String, Object>> actionsMapList = (List<Map<String, Object>>) actionsObject;
            if (actionsMapList.isEmpty()) {
                leastCommonRemovableSettings = null;
                break;
            }
            for (Map<String, Object> actionsMap : actionsMapList) {
                if (REMOVE_SETTINGS_ACTION_TYPE.equals(actionsMap.get(ACTION_TYPE))) {
                    List<String> removableSettings = (List<String>) actionsMap.get(OBJECTS_FIELD);
                    hasDeletableSettings = true;
                    if (leastCommonRemovableSettings == null) {
                        leastCommonRemovableSettings = new ArrayList<>(removableSettings);
                    } else {
                        leastCommonRemovableSettings = leastCommonRemovableSettings.stream()
                            .distinct()
                            .filter(removableSettings::contains)
                            .collect(Collectors.toList());
                    }
                }
            }
        }
        DeprecationIssue representativeIssue = similarIssues.get(0);
        Map<String, Object> representativeMeta = representativeIssue.getMeta();
        final Map<String, Object> newMeta;
        if (representativeMeta != null) {
            newMeta = new HashMap<>(representativeMeta);
            if (hasDeletableSettings) {
                List<Map<String, Object>> actions = (List<Map<String, Object>>) newMeta.get(ACTIONS_META_FIELD);
                List<Map<String, Object>> clonedActions = new ArrayList<>(actions);
                newMeta.put(ACTIONS_META_FIELD, clonedActions); // So that we don't modify the input data
                for (int i = 0; i < clonedActions.size(); i++) {
                    Map<String, Object> actionsMap = clonedActions.get(i);
                    if (REMOVE_SETTINGS_ACTION_TYPE.equals(actionsMap.get(ACTION_TYPE))) {
                        Map<String, Object> clonedActionsMap = new HashMap<>(actionsMap);
                        if (leastCommonRemovableSettings == null) {
                            clonedActionsMap.put(OBJECTS_FIELD, Collections.emptyList());
                        } else {
                            clonedActionsMap.put(OBJECTS_FIELD, leastCommonRemovableSettings);
                        }
                        clonedActions.set(i, clonedActionsMap);
                    }
                }
                for (Map<String, Object> actionsMap : clonedActions) {
                    if (REMOVE_SETTINGS_ACTION_TYPE.equals(actionsMap.get(ACTION_TYPE))) {
                        if (leastCommonRemovableSettings == null) {
                            actionsMap.put(OBJECTS_FIELD, Collections.emptyList());
                        } else {
                            actionsMap.put(OBJECTS_FIELD, leastCommonRemovableSettings);
                        }
                    }
                }
            }
        } else {
            newMeta = null;
        }
        return new DeprecationIssue(
            representativeIssue.level,
            representativeIssue.message,
            representativeIssue.url,
            representativeIssue.details,
            representativeIssue.resolveDuringRollingUpgrade,
            newMeta
        );
    }
}
