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

import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.stream.StreamSupport;

public class FileStoreInstrumentation implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        EntitlementRulesBuilder builder = new EntitlementRulesBuilder(registry);

        var fileStoreClasses = StreamSupport.stream(FileSystems.getDefault().getFileStores().spliterator(), false)
            .map(FileStore::getClass)
            .distinct()
            .toList();

        builder.on(fileStoreClasses, rule -> {
            rule.calling(FileStore::getFileStoreAttributeView, new TypeToken<Class<? extends FileStoreAttributeView>>() {})
                .enforce(Policies::getFileAttributeView)
                .elseThrowNotEntitled();
            rule.calling(FileStore::getAttribute, String.class).enforce(Policies::readStoreAttributes).elseThrowNotEntitled();
            rule.calling(FileStore::getBlockSize).enforce(Policies::readStoreAttributes).elseThrowNotEntitled();
            rule.calling(FileStore::getTotalSpace).enforce(Policies::readStoreAttributes).elseThrowNotEntitled();
            rule.calling(FileStore::getUnallocatedSpace).enforce(Policies::readStoreAttributes).elseThrowNotEntitled();
            rule.calling(FileStore::getUsableSpace).enforce(Policies::readStoreAttributes).elseThrowNotEntitled();
            rule.calling(FileStore::isReadOnly).enforce(Policies::readStoreAttributes).elseThrowNotEntitled();
            rule.calling(FileStore::name).enforce(Policies::readStoreAttributes).elseThrowNotEntitled();
            rule.calling(FileStore::type).enforce(Policies::readStoreAttributes).elseThrowNotEntitled();
        });
    }
}
