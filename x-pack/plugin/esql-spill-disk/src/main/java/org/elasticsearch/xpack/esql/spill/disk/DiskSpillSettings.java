/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spill.disk;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.RelativeByteSizeValue;

/**
 * The module's settings. The path is static because the entitlement that grants it is resolved when the node starts;
 * the cap is dynamic, a lowering keeps existing runs readable and may fail a running query's next write.
 */
public final class DiskSpillSettings {

    /**
     * Where runs go; empty means {@code esql_spill} under the node's first data path.
     */
    public static final Setting<String> PATH = Setting.simpleString("esql.spill.disk.path", Setting.Property.NodeScope);

    /**
     * The most this node may hold on the spill path across every query, absolute or a ratio of that path's file system.
     */
    public static final Setting<RelativeByteSizeValue> CAP = new Setting<>(
        "esql.spill.disk.cap",
        "10%",
        value -> RelativeByteSizeValue.parseRelativeByteSizeValue(value, "esql.spill.disk.cap"),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final String DEFAULT_DIRECTORY = "esql_spill";

    private DiskSpillSettings() {}
}
