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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class JvmInfo implements Streamable, Serializable, ToXContent {

    private static JvmInfo INSTANCE;

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
        JvmInfo info = new JvmInfo();
        info.pid = pid;
        info.startTime = runtimeMXBean.getStartTime();
        info.version = runtimeMXBean.getSystemProperties().get("java.version");
        info.vmName = runtimeMXBean.getVmName();
        info.vmVendor = runtimeMXBean.getVmVendor();
        info.vmVersion = runtimeMXBean.getVmVersion();
        info.mem = new Mem();
        info.mem.heapInit = memoryMXBean.getHeapMemoryUsage().getInit();
        info.mem.heapMax = memoryMXBean.getHeapMemoryUsage().getMax();
        info.mem.nonHeapInit = memoryMXBean.getNonHeapMemoryUsage().getInit();
        info.mem.nonHeapMax = memoryMXBean.getNonHeapMemoryUsage().getMax();
        info.inputArguments = runtimeMXBean.getInputArguments().toArray(new String[runtimeMXBean.getInputArguments().size()]);
        info.bootClassPath = runtimeMXBean.getBootClassPath();
        info.classPath = runtimeMXBean.getClassPath();
        info.systemProperties = runtimeMXBean.getSystemProperties();

        INSTANCE = info;
    }

    public static JvmInfo jvmInfo() {
        return INSTANCE;
    }

    long pid = -1;

    String version = "";
    String vmName = "";
    String vmVersion = "";
    String vmVendor = "";

    long startTime = -1;

    Mem mem;

    String[] inputArguments;

    String bootClassPath;

    String classPath;

    Map<String, String> systemProperties;

    private JvmInfo() {
    }

    /**
     * The process id.
     */
    public long pid() {
        return this.pid;
    }

    /**
     * The process id.
     */
    public long getPid() {
        return pid;
    }

    public String version() {
        return this.version;
    }

    public String getVersion() {
        return this.version;
    }

    public String vmName() {
        return vmName;
    }

    public String getVmName() {
        return vmName;
    }

    public String vmVersion() {
        return vmVersion;
    }

    public String getVmVersion() {
        return vmVersion;
    }

    public String vmVendor() {
        return vmVendor;
    }

    public String getVmVendor() {
        return vmVendor;
    }

    public long startTime() {
        return startTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public Mem mem() {
        return mem;
    }

    public Mem getMem() {
        return mem();
    }

    public String[] inputArguments() {
        return inputArguments;
    }

    public String[] getInputArguments() {
        return inputArguments;
    }

    public String bootClassPath() {
        return bootClassPath;
    }

    public String getBootClassPath() {
        return bootClassPath;
    }

    public String classPath() {
        return classPath;
    }

    public String getClassPath() {
        return classPath;
    }

    public Map<String, String> systemProperties() {
        return systemProperties;
    }

    public Map<String, String> getSystemProperties() {
        return systemProperties;
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("jvm");
        builder.field("pid", pid);
        builder.field("version", version);
        builder.field("vm_name", vmName);
        builder.field("vm_version", vmVersion);
        builder.field("vm_vendor", vmVendor);
        builder.field("start_time", startTime);

        builder.startObject("mem");
        builder.field("heap_init", mem.heapInit().toString());
        builder.field("heap_init_in_bytes", mem.heapInit);
        builder.field("heap_max", mem.heapMax().toString());
        builder.field("heap_max_in_bytes", mem.heapMax);
        builder.field("non_heap_init", mem.nonHeapInit().toString());
        builder.field("non_heap_init_in_bytes", mem.nonHeapInit);
        builder.field("non_heap_max", mem.nonHeapMax().toString());
        builder.field("non_heap_max_in_bytes", mem.nonHeapMax);
        builder.endObject();

        builder.endObject();
        return builder;
    }

    public static JvmInfo readJvmInfo(StreamInput in) throws IOException {
        JvmInfo jvmInfo = new JvmInfo();
        jvmInfo.readFrom(in);
        return jvmInfo;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        pid = in.readLong();
        version = in.readUTF();
        vmName = in.readUTF();
        vmVersion = in.readUTF();
        vmVendor = in.readUTF();
        startTime = in.readLong();
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
        mem = new Mem();
        mem.readFrom(in);
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(pid);
        out.writeUTF(version);
        out.writeUTF(vmName);
        out.writeUTF(vmVersion);
        out.writeUTF(vmVendor);
        out.writeLong(startTime);
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
        mem.writeTo(out);
    }

    public static class Mem implements Streamable, Serializable {

        long heapInit = -1;
        long heapMax = -1;
        long nonHeapInit = -1;
        long nonHeapMax = -1;

        Mem() {
        }

        public ByteSizeValue heapInit() {
            return new ByteSizeValue(heapInit);
        }

        public ByteSizeValue getHeapInit() {
            return heapInit();
        }

        public ByteSizeValue heapMax() {
            return new ByteSizeValue(heapMax);
        }

        public ByteSizeValue getHeapMax() {
            return heapMax();
        }

        public ByteSizeValue nonHeapInit() {
            return new ByteSizeValue(nonHeapInit);
        }

        public ByteSizeValue getNonHeapInit() {
            return nonHeapInit();
        }

        public ByteSizeValue nonHeapMax() {
            return new ByteSizeValue(nonHeapMax);
        }

        public ByteSizeValue getNonHeapMax() {
            return nonHeapMax();
        }

        public static Mem readMem(StreamInput in) throws IOException {
            Mem mem = new Mem();
            mem.readFrom(in);
            return mem;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            heapInit = in.readVLong();
            heapMax = in.readVLong();
            nonHeapInit = in.readVLong();
            nonHeapMax = in.readVLong();
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(heapInit);
            out.writeVLong(heapMax);
            out.writeVLong(nonHeapInit);
            out.writeVLong(nonHeapMax);
        }
    }
}
