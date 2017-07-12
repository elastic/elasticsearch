/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.protocol.shared;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Request general information about the server.
 */
public abstract class AbstractInfoRequest extends Request {
    public final String jvmVersion, jvmVendor, jvmClassPath, osName, osVersion;

    /**
     * Build the info request containing information about the current JVM.
     */
    protected AbstractInfoRequest() {
        jvmVersion = System.getProperty("java.version", "");
        jvmVendor = System.getProperty("java.vendor", "");
        jvmClassPath = System.getProperty("java.class.path", "");
        osName = System.getProperty("os.name", "");
        osVersion = System.getProperty("os.version", "");
    }

    protected AbstractInfoRequest(String jvmVersion, String jvmVendor, String jvmClassPath, String osName, String osVersion) {
        this.jvmVersion = jvmVersion;
        this.jvmVendor = jvmVendor;
        this.jvmClassPath = jvmClassPath;
        this.osName = osName;
        this.osVersion = osVersion;
    }

    protected AbstractInfoRequest(int clientVersion, DataInput in) throws IOException {
        jvmVersion = in.readUTF();
        jvmVendor = in.readUTF();
        jvmClassPath = in.readUTF();
        osName = in.readUTF();
        osVersion = in.readUTF();
    }

    @Override
    public final void write(DataOutput out) throws IOException {
        out.writeUTF(jvmVersion);
        out.writeUTF(jvmVendor);
        out.writeUTF(jvmClassPath);
        out.writeUTF(osName);
        out.writeUTF(osVersion);
    }

    @Override
    protected final String toStringBody() {
        return "jvm=[version=[" + jvmVersion
                + "] vendor=[" + jvmVendor
                + "] classPath=[" + jvmClassPath
                + "]] os=[name=[" + osName
                + "] version=[" + osVersion + "]]";
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        AbstractInfoRequest other = (AbstractInfoRequest) obj;
        return Objects.equals(jvmVersion, other.jvmVersion)
                && Objects.equals(jvmVendor, other.jvmVendor)
                && Objects.equals(jvmClassPath, other.jvmClassPath)
                && Objects.equals(osName, other.osName)
                && Objects.equals(osVersion, other.osVersion);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(jvmVersion, jvmVendor, jvmClassPath, osName, osVersion);
    }
}
