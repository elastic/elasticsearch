/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.reservedstate.service.ReservedStateVersion;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentType.JSON;

public class LocallyMountedSecrets implements SecureSettings {
    public static final String SECRETS_FILE_NAME = "secrets.json";
    public static final String SECRETS_DIRECTORY = "secrets";

    public static final ParseField SECRETS_FIELD = new ParseField("secrets");
    public static final ParseField METADATA_FIELD = new ParseField("metadata");

    @SuppressWarnings("unchecked")
    private final ConstructingObjectParser<LocalFileSecrets, Void> secretsParser = new ConstructingObjectParser<>(
        "reserved_state_chunk",
        a -> new LocalFileSecrets((Map<String, String>) a[0], (ReservedStateVersion) a[1])
    );

    private final String secretsDir;
    private final String secretsFile;
    private final SetOnce<LocalFileSecrets> secrets = new SetOnce<>();
    private volatile boolean closed;

    public LocallyMountedSecrets(Environment environment) {
        var secretsDirPath = environment.configFile().toAbsolutePath().resolve(SECRETS_DIRECTORY);
        var secretsFilePath = secretsDirPath.resolve(SECRETS_FILE_NAME);
        secretsParser.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.map(), SECRETS_FIELD);
        secretsParser.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> ReservedStateVersion.parse(p), METADATA_FIELD);
        if (Files.exists(secretsDirPath) && Files.exists(secretsFilePath)) {
            try {
                secrets.set(processSecretsFile(secretsFilePath));
                closed = false;
            } catch (IOException e) {
                throw new IllegalStateException("Error processing secrets file", e);
            }
        }
        this.secretsDir = secretsDirPath.toString();
        this.secretsFile = secretsFilePath.toString();
    }

    public LocallyMountedSecrets(StreamInput in) throws IOException {
        this.secretsDir = in.readString();
        this.secretsFile = in.readString();
        if (in.readBoolean()) {
            secrets.set(LocalFileSecrets.readFrom(in));
        }
        this.closed = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(secretsDir);
        out.writeString(secretsFile);
        if (secrets.get() == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            secrets.get().writeTo(out);
        }
        out.writeBoolean(closed);
    }

    @Override
    public boolean isLoaded() {
        return secrets.get() != null;
    }

    @Override
    public Set<String> getSettingNames() {
        assert isLoaded();
        return secrets.get().map().keySet();
    }

    @Override
    public SecureString getString(String setting) {
        assert isLoaded();
        return new SecureString(secrets.get().map().get(setting).toString().toCharArray());
    }

    @Override
    public InputStream getFile(String setting) throws GeneralSecurityException {
        return null;
    }

    @Override
    public byte[] getSHA256Digest(String setting) throws GeneralSecurityException {
        return new byte[0];
    }

    @Override
    public void close() throws IOException {
        closed = true;
        if (null != secrets.get() && secrets.get().map().isEmpty() == false) {
            for (var entry : secrets.get().map().entrySet()) {
                entry.setValue(null);
            }
        }
    }

    LocalFileSecrets processSecretsFile(Path path) throws IOException {
        try (
            var fis = Files.newInputStream(path);
            var bis = new BufferedInputStream(fis);
            var parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bis)
        ) {
            return secretsParser.apply(parser, null);
        }
    }

    record LocalFileSecrets(Map<String, String> map, ReservedStateVersion metadata) implements Writeable {
        public static LocalFileSecrets readFrom(StreamInput in) throws IOException {
            return new LocalFileSecrets(in.readMap(StreamInput::readString, StreamInput::readString), ReservedStateVersion.readFrom(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap((map == null) ? Map.of() : map, StreamOutput::writeString, StreamOutput::writeString);
            metadata.writeTo(out);
        }
    }
}
