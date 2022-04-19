/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;

import java.io.IOException;
import java.nio.file.Path;

public record ServerArgs(boolean daemonize, Path pidFile, Settings nodeSettings, Path configDir) implements Writeable {

    public ServerArgs(StreamInput in) throws IOException {
        this(in.readBoolean(), readPidFile(in), Settings.readSettingsFromStream(in), PathUtils.get(in.readString()));
    }

    private static Path readPidFile(StreamInput in) throws IOException {
        String pidFile = in.readOptionalString();
        return pidFile == null ? null : PathUtils.get(pidFile);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(daemonize);
        out.writeOptionalString(pidFile == null ? null : pidFile.toString());
        Settings.writeSettingsToStream(nodeSettings, out);
        out.writeString(configDir.toString());
    }
}
