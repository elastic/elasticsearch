/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import org.elasticsearch.entitlement.rules.EntitlementRulesBuilder;
import org.elasticsearch.entitlement.rules.Policies;
import org.elasticsearch.entitlement.rules.TypeToken;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.attribute.FileStoreAttributeView;

public class FileStoreInstrumentation implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        EntitlementRulesBuilder builder = new EntitlementRulesBuilder(registry);

        builder.on(FileStore.class, rule -> {
            rule.calling(FileStore::getFileStoreAttributeView, new TypeToken<Class<? extends FileStoreAttributeView>>() {})
                .enforce(Policies::getFileAttributeView)
                .elseReturn(null);
            rule.calling(FileStore::getAttribute, String.class).enforce(Policies::readStoreAttributes).elseThrow(IOException::new);
            rule.calling(FileStore::getBlockSize).enforce(Policies::readStoreAttributes).elseThrow(IOException::new);
            rule.calling(FileStore::getTotalSpace).enforce(Policies::readStoreAttributes).elseThrow(IOException::new);
            rule.calling(FileStore::getUnallocatedSpace).enforce(Policies::readStoreAttributes).elseThrow(IOException::new);
            rule.calling(FileStore::getUsableSpace).enforce(Policies::readStoreAttributes).elseThrow(IOException::new);
            rule.calling(FileStore::isReadOnly).enforce(Policies::readStoreAttributes).elseReturn(true);
            rule.calling(FileStore::name).enforce(Policies::readStoreAttributes).elseThrowNotEntitled();
            rule.calling(FileStore::type).enforce(Policies::readStoreAttributes).elseThrowNotEntitled();
        });
    }
}
