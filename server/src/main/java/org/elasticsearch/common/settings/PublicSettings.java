/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

public interface PublicSettings {
    Settings filterPublic(Settings settings);

    void validateSettings(Settings settings);

    class DefaultPublicSettings implements PublicSettings {
        @Override
        public Settings filterPublic(Settings settings) {
            return settings;
        }

        @Override
        public void validateSettings(Settings settings) {

        }
    }
}
