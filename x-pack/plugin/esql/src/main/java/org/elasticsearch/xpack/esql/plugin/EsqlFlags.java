/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;

public class EsqlFlags {
    public static final Setting<Boolean> ESQL_STRING_LIKE_ON_INDEX = Setting.boolSetting(
        "esql.query.string_like_on_index",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final boolean stringLikeOnIndex;

    public EsqlFlags(boolean stringLikeOnIndex) {
        this.stringLikeOnIndex = stringLikeOnIndex;
    }

    public EsqlFlags(ClusterSettings settings) {
        this.stringLikeOnIndex = settings.get(ESQL_STRING_LIKE_ON_INDEX);
    }

    public boolean stringLikeOnIndex() {
        return stringLikeOnIndex;
    }
}
