/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchKey;
import java.nio.file.attribute.BasicFileAttributes;

import static org.elasticsearch.xcontent.XContentType.JSON;

// Settings have a path, a file update state, and a watch key
class WatchableFileSettings {

    private static final Logger logger = LogManager.getLogger(WatchableFileSettings.class);

    private final FileSettingsService fileSettingsService;
    final Path operatorSettingsDir;
    String settingsFileName;
    FileSettingsService.FileUpdateState fileUpdateState;
    WatchKey settingsDirWatchKey;

    WatchableFileSettings(FileSettingsService fileSettingsService, Path operatorSettingsDir) {
        this.fileSettingsService = fileSettingsService;
        this.operatorSettingsDir = operatorSettingsDir;
    }

    // platform independent way to tell if a file changed
    // we compare the file modified timestamp, the absolute path (symlinks), and file id on the system
    boolean watchedFileChanged(Path path) throws IOException {
        if (Files.exists(path) == false) {
            return false;
        }

        FileSettingsService.FileUpdateState previousUpdateState = fileUpdateState;

        BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
        fileUpdateState = new FileSettingsService.FileUpdateState(
            attr.lastModifiedTime().toMillis(),
            path.toRealPath().toString(),
            attr.fileKey()
        );

        return (previousUpdateState == null
            || previousUpdateState.equals(fileUpdateState) == false);
    }

    PlainActionFuture<Void> processFileSettings(Path path) {
        PlainActionFuture<Void> completion = PlainActionFuture.newFuture();
        logger.info("processing path [{}] for [{}]", path, FileSettingsService.NAMESPACE);
        try (
            var fis = Files.newInputStream(path);
            var bis = new BufferedInputStream(fis);
            var parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bis)
        ) {
            fileSettingsService.getStateService().process(FileSettingsService.NAMESPACE, parser, (e) -> completeProcessing(e, completion));
        } catch (Exception e) {
            completion.onFailure(e);
        }

        return completion;
    }

    private void completeProcessing(Exception e, PlainActionFuture<Void> completion) {
        if (e != null) {
            completion.onFailure(e);
        } else {
            completion.onResponse(null);
        }
    }

}
