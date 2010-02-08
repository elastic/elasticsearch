/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.monitor.jvm;

import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class JvmConfig implements Streamable, Serializable {

    private static JvmConfig INSTANCE;

    static {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

        // returns the <process id>@<host>
        long pid;
        String xPid = runtimeMXBean.getName();
        try {
            xPid = xPid.split("@")[0];
            pid = Long.parseLong(xPid);
        } catch (Exception e) {
            pid = -1;
        }
        INSTANCE = new JvmConfig(pid, runtimeMXBean.getVmName(), System.getProperty("java.version"), System.getProperty("java.vendor"),
                runtimeMXBean.getStartTime(),
                memoryMXBean.getHeapMemoryUsage().getInit(), memoryMXBean.getHeapMemoryUsage().getMax(),
                memoryMXBean.getNonHeapMemoryUsage().getInit(), memoryMXBean.getNonHeapMemoryUsage().getMax(),
                runtimeMXBean.getInputArguments().toArray(new String[runtimeMXBean.getInputArguments().size()]), runtimeMXBean.getBootClassPath(), runtimeMXBean.getClassPath(), runtimeMXBean.getSystemProperties());
    }

    public static JvmConfig jvmConfig() {
        return INSTANCE;
    }

    private long pid = -1;

    private String vmName = "";

    private String vmVersion = "";

    private String vmVendor = "";

    private long startTime = -1;

    private long memoryHeapInit = -1;

    private long memoryHeapMax = -1;

    private long memoryNonHeapInit = -1;

    private long memoryNonHeapMax = -1;

    private String[] inputArguments;

    private String bootClassPath;

    private String classPath;

    private Map<String, String> systemProperties;

    private JvmConfig() {
    }

    public JvmConfig(long pid, String vmName, String vmVersion, String vmVendor, long startTime,
                     long memoryHeapInit, long memoryHeapMax, long memoryNonHeapInit, long memoryNonHeapMax,
                     String[] inputArguments, String bootClassPath, String classPath, Map<String, String> systemProperties) {
        this.pid = pid;
        this.vmName = vmName;
        this.vmVersion = vmVersion;
        this.vmVendor = vmVendor;
        this.startTime = startTime;
        this.memoryHeapInit = memoryHeapInit;
        this.memoryHeapMax = memoryHeapMax;
        this.memoryNonHeapInit = memoryNonHeapInit;
        this.memoryNonHeapMax = memoryNonHeapMax;
        this.inputArguments = inputArguments;
        this.bootClassPath = bootClassPath;
        this.classPath = classPath;
        this.systemProperties = systemProperties;
    }

    public long pid() {
        return this.pid;
    }

    public String vmName() {
        return vmName;
    }

    public String vmVersion() {
        return vmVersion;
    }

    public String vmVendor() {
        return vmVendor;
    }

    public long startTime() {
        return startTime;
    }

    public SizeValue memoryHeapInit() {
        return new SizeValue(memoryHeapInit);
    }

    public SizeValue memoryHeapMax() {
        return new SizeValue(memoryHeapMax);
    }

    public SizeValue memoryNonHeapInit() {
        return new SizeValue(memoryNonHeapInit);
    }

    public SizeValue memoryNonHeapMax() {
        return new SizeValue(memoryNonHeapMax);
    }

    public String[] inputArguments() {
        return inputArguments;
    }

    public String bootClassPath() {
        return bootClassPath;
    }

    public String classPath() {
        return classPath;
    }

    public Map<String, String> systemProperties() {
        return systemProperties;
    }

    public static JvmConfig readJvmComing(DataInput in) throws IOException, ClassNotFoundException {
        JvmConfig jvmConfig = new JvmConfig();
        jvmConfig.readFrom(in);
        return jvmConfig;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        pid = in.readLong();
        vmName = in.readUTF();
        vmVersion = in.readUTF();
        vmVendor = in.readUTF();
        startTime = in.readLong();
        memoryHeapInit = in.readLong();
        memoryHeapMax = in.readLong();
        memoryNonHeapInit = in.readLong();
        memoryNonHeapMax = in.readLong();
        inputArguments = new String[in.readInt()];
        for (int i = 0; i < inputArguments.length; i++) {
            inputArguments[i] = in.readUTF();
        }
        bootClassPath = in.readUTF();
        classPath = in.readUTF();
        systemProperties = new HashMap<String, String>();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            systemProperties.put(in.readUTF(), in.readUTF());
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeLong(pid);
        out.writeUTF(vmName);
        out.writeUTF(vmVersion);
        out.writeUTF(vmVendor);
        out.writeLong(startTime);
        out.writeLong(memoryHeapInit);
        out.writeLong(memoryHeapMax);
        out.writeLong(memoryNonHeapInit);
        out.writeLong(memoryNonHeapMax);
        out.writeInt(inputArguments.length);
        for (String inputArgument : inputArguments) {
            out.writeUTF(inputArgument);
        }
        out.writeUTF(bootClassPath);
        out.writeUTF(classPath);
        out.writeInt(systemProperties.size());
        for (Map.Entry<String, String> entry : systemProperties.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }
}
