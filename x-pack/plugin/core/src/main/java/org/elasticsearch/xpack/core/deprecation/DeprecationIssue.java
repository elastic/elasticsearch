/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.deprecation;

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
import java.util.Optional;
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
        resolveDuringRollingUpgrade = in.readBoolean();
        meta = in.readMap();
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

    private Optional<Meta> getMetaObject() {
        return Meta.fromMetaMap(meta);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        level.writeTo(out);
        out.writeString(message);
        out.writeString(url);
        out.writeOptionalString(details);
        out.writeBoolean(resolveDuringRollingUpgrade);
        out.writeGenericMap(meta);
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
        return Meta.fromRemovableSettings(removableSettings).toMetaMap();
    }

    /**
     * This method returns a DeprecationIssue that has in its meta object the intersection of all auto-removable settings that appear on
     * all of the DeprecationIssues that are passed in. This method assumes that all DeprecationIssues passed in are equal, except for the
     * auto-removable settings in the meta object.
     * @param similarIssues DeprecationIssues that are assumed to be identical except possibly removal actions.
     * @return A DeprecationIssue containing only the removal actions that are in all similarIssues
     */
    public static DeprecationIssue getIntersectionOfRemovableSettings(List<DeprecationIssue> similarIssues) {
        if (similarIssues == null || similarIssues.isEmpty()) {
            return null;
        }
        if (similarIssues.size() == 1) {
            return similarIssues.get(0);
        }
        DeprecationIssue representativeIssue = similarIssues.get(0);
        Optional<Meta> metaIntersection = similarIssues.stream()
            .map(DeprecationIssue::getMetaObject)
            .reduce(
                representativeIssue.getMetaObject(),
                (intersectionSoFar, meta) -> intersectionSoFar.isPresent() && meta.isPresent()
                    ? Optional.of(intersectionSoFar.get().getIntersection(meta.get()))
                    : Optional.empty()
            );
        return new DeprecationIssue(
            representativeIssue.level,
            representativeIssue.message,
            representativeIssue.url,
            representativeIssue.details,
            representativeIssue.resolveDuringRollingUpgrade,
            metaIntersection.map(Meta::toMetaMap).orElse(null)
        );
    }

    /*
     * This class a represents a DeprecationIssue's meta map. A meta map might look something like:
     * {
     *    "_meta":{
     *       "foo": "bar",
     *       "actions":[
     *          {
     *             "action_type":"remove_settings",
     *             "objects":[
     *                "setting1",
     *                "setting2"
     *             ]
     *          }
     *       ]
     *    }
     * }
     */
    private static final class Meta {
        private final List<Action> actions;
        private final Map<String, Object> nonActionMetadata;

        Meta(List<Action> actions, Map<String, Object> nonActionMetadata) {
            this.actions = actions;
            this.nonActionMetadata = nonActionMetadata;
        }

        private static Meta fromRemovableSettings(List<String> removableSettings) {
            List<Action> actions;
            if (removableSettings == null) {
                actions = null;
            } else {
                actions = Collections.singletonList(new RemovalAction(removableSettings));
            }
            return new Meta(actions, Collections.emptyMap());
        }

        private Map<String, Object> toMetaMap() {
            Map<String, Object> metaMap;
            if (actions != null) {
                metaMap = new HashMap<>(nonActionMetadata);
                List<Map<String, Object>> actionsList = actions.stream().map(Action::toActionMap).collect(Collectors.toList());
                if (actionsList.isEmpty() == false) {
                    metaMap.put(ACTIONS_META_FIELD, actionsList);
                }
            } else {
                metaMap = nonActionMetadata;
            }
            return metaMap;
        }

        /*
         * This method gets the intersection of this Meta with another. It assumes that the Meta objects are identical, except possibly the
         * contents of the removal actions. So the interection is a new Meta object with only the removal actions that appear in both.
         */
        private Meta getIntersection(Meta another) {
            final List<Action> actionsIntersection;
            if (actions != null && another.actions != null) {
                List<Action> combinedActions = this.actions.stream()
                    .filter(action -> action instanceof RemovalAction == false)
                    .collect(Collectors.toList());
                Optional<Action> thisRemovalAction = this.actions.stream().filter(action -> action instanceof RemovalAction).findFirst();
                Optional<Action> otherRemovalAction = another.actions.stream()
                    .filter(action -> action instanceof RemovalAction)
                    .findFirst();
                if (thisRemovalAction.isPresent() && otherRemovalAction.isPresent()) {
                    Optional<List<String>> removableSettingsOptional = ((RemovalAction) thisRemovalAction.get()).getRemovableSettings();
                    List<String> removableSettings = removableSettingsOptional.map(
                        settings -> settings.stream()
                            .distinct()
                            .filter(
                                setting -> ((RemovalAction) otherRemovalAction.get()).getRemovableSettings()
                                    .map(list -> list.contains(setting))
                                    .orElse(false)
                            )
                            .collect(Collectors.toList())
                    ).orElse(Collections.emptyList());
                    if (removableSettings.isEmpty() == false) {
                        combinedActions.add(new RemovalAction(removableSettings));
                    }
                }
                actionsIntersection = combinedActions;
            } else {
                actionsIntersection = null;
            }
            return new Meta(actionsIntersection, nonActionMetadata);
        }

        /*
         * Returns an Optional Meta object from a DeprecationIssue's meta Map. If the meta Map is null then the Optional will not be
         * present.
         */
        @SuppressWarnings("unchecked")
        private static Optional<Meta> fromMetaMap(Map<String, Object> metaMap) {
            if (metaMap == null) {
                return Optional.empty();
            }
            List<Map<String, Object>> actionMaps = (List<Map<String, Object>>) metaMap.get(ACTIONS_META_FIELD);
            List<Action> actions;
            if (actionMaps == null) {
                actions = null;
            } else {
                actions = new ArrayList<>();
                for (Map<String, Object> actionMap : actionMaps) {
                    final Action action;
                    if (REMOVE_SETTINGS_ACTION_TYPE.equals(actionMap.get(ACTION_TYPE))) {
                        action = RemovalAction.fromActionMap(actionMap);
                    } else {
                        action = UnknownAction.fromActionMap(actionMap);
                    }
                    actions.add(action);
                }
            }
            Map<String, Object> nonActionMap = metaMap.entrySet()
                .stream()
                .filter(entry -> entry.getKey().equals(ACTIONS_META_FIELD) == false)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            Meta meta = new Meta(actions, nonActionMap);
            return Optional.of(meta);
        }
    }

    /*
     * A DeprecationIssue's meta Map optionally has an array of actions. This class reprenents one of the items in that array.
     */
    private interface Action {
        /*
         * This method creates the Map that goes inside the actions list for this Action in a meta Map.
         */
        Map<String, Object> toActionMap();
    }

    /*
     * This class a represents remove_settings action within the actions list in a meta Map.
     */
    private static final class RemovalAction implements Action {
        private final List<String> removableSettings;

        RemovalAction(List<String> removableSettings) {
            this.removableSettings = removableSettings;
        }

        @SuppressWarnings("unchecked")
        private static RemovalAction fromActionMap(Map<String, Object> actionMap) {
            final List<String> removableSettings;
            Object removableSettingsObject = actionMap.get(OBJECTS_FIELD);
            if (removableSettingsObject == null) {
                removableSettings = null;
            } else {
                removableSettings = (List<String>) removableSettingsObject;
            }
            return new RemovalAction(removableSettings);
        }

        private Optional<List<String>> getRemovableSettings() {
            return removableSettings == null ? Optional.empty() : Optional.of(removableSettings);
        }

        @Override
        public Map<String, Object> toActionMap() {
            final Map<String, Object> actionMap;
            if (removableSettings != null) {
                actionMap = new HashMap<>();
                actionMap.put(OBJECTS_FIELD, removableSettings);
                actionMap.put(ACTION_TYPE, REMOVE_SETTINGS_ACTION_TYPE);
            } else {
                actionMap = null;
            }
            return actionMap;
        }
    }

    /*
     * This represents an action within the actions list in a meta Map that is *not* a removal_action.
     */
    private static class UnknownAction implements Action {
        private final Map<String, Object> actionMap;

        private UnknownAction(Map<String, Object> actionMap) {
            this.actionMap = actionMap;
        }

        private static Action fromActionMap(Map<String, Object> actionMap) {
            return new UnknownAction(actionMap);
        }

        @Override
        public Map<String, Object> toActionMap() {
            return actionMap;
        }
    }
}
