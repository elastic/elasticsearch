/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.Action;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

public class InfoRequest extends Request {
    private static final String EMPTY = "";

    public final String jvmVersion, jvmVendor, jvmClassPath, osName, osVersion;

    public InfoRequest(Properties props) {
        super(Action.INFO);
        jvmVersion = props.getProperty("java.version", EMPTY);
        jvmVendor = props.getProperty("java.vendor", EMPTY);
        jvmClassPath = props.getProperty("java.class.path", EMPTY);
        osName = props.getProperty("os.name", EMPTY);
        osVersion = props.getProperty("os.version", EMPTY);
    }

    public InfoRequest(String jvmVersion, String jvmVendor, String jvmClassPath, String osName, String osVersion) {
        super(Action.INFO);
        this.jvmVersion = jvmVersion;
        this.jvmVendor = jvmVendor;
        this.jvmClassPath = jvmClassPath;
        this.osName = osName;
        this.osVersion = osVersion;
    }

    @Override
    public void encode(DataOutput out) throws IOException {
        out.writeInt(action.value());
        out.writeUTF(jvmVersion);
        out.writeUTF(jvmVendor);
        out.writeUTF(jvmClassPath);
        out.writeUTF(osName);
        out.writeUTF(osVersion);
    }

    public static InfoRequest decode(DataInput in) throws IOException {
        String jvmVersion = in.readUTF();
        String jvmVendor = in.readUTF();
        String jvmClassPath = in.readUTF();
        String osName = in.readUTF();
        String osVersion = in.readUTF();

        return new InfoRequest(jvmVersion, jvmVendor, jvmClassPath, osName, osVersion);
    }
}
