/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.util.Map;

/**
 * An {@link EmptyTaskSettings} variant that rejects (rather than silently drops) unknown settings when updated.
 *
 * <p>This is used while a cluster is not yet fully upgraded to support a new task setting (e.g. {@code reasoning}
 * for Elastic Inference Service completion): the settings must still be empty for BWC, but a live update carrying
 * the new field should be rejected rather than accepted and silently discarded. {@link EmptyTaskSettings#updatedTaskSettings}
 * unconditionally accepts and ignores any input, which is exactly the behavior that must not apply here.
 *
 * <p>This class deliberately does not override {@link #getWriteableName()} or {@link #writeTo}, so it serializes
 * byte-for-byte like {@link EmptyTaskSettings} and is not registered as a separate named writeable: any node, old
 * or new, reads it back as a plain {@link EmptyTaskSettings}. That's safe because the enforcement only needs to
 * hold within a single update request — the "existing settings" object consulted during an update is always
 * freshly reconstructed from the persisted model configuration in the same request, never deserialized from the
 * wire, so a fresh {@link EnforcingEmptyTaskSettings} is produced again on the next request regardless.
 */
public class EnforcingEmptyTaskSettings extends EmptyTaskSettings {

    public static final EnforcingEmptyTaskSettings INSTANCE = new EnforcingEmptyTaskSettings();

    // Package-private (rather than private) so tests in this package can construct a fresh, equal instance
    // distinct from INSTANCE (e.g. to verify serialization round-trips produce an equal-but-different object).
    EnforcingEmptyTaskSettings() {
        super();
    }

    public EnforcingEmptyTaskSettings(StreamInput in) {
        super(in);
    }

    public static TaskSettings fromMap(Map<String, Object> settings, ConfigurationParseContext context) {
        if (settings.isEmpty() || context == ConfigurationParseContext.PERSISTENT) {
            return INSTANCE;
        }

        throw new ElasticsearchStatusException(
            "[{}] Configuration contains unknown settings {}",
            RestStatus.BAD_REQUEST,
            ModelConfigurations.TASK_SETTINGS,
            settings.keySet()
        );
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return fromMap(newSettings, ConfigurationParseContext.REQUEST);
    }
}
