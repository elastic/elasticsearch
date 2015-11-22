/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.lang.management.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class JvmInfo implements Streamable, ToXContent {

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
        info.version = System.getProperty("java.version");
        info.vmName = runtimeMXBean.getVmName();
        info.vmVendor = runtimeMXBean.getVmVendor();
        info.vmVersion = runtimeMXBean.getVmVersion();
        info.mem = new Mem();
        info.mem.heapInit = memoryMXBean.getHeapMemoryUsage().getInit() < 0 ? 0 : memoryMXBean.getHeapMemoryUsage().getInit();
        info.mem.heapMax = memoryMXBean.getHeapMemoryUsage().getMax() < 0 ? 0 : memoryMXBean.getHeapMemoryUsage().getMax();
        info.mem.nonHeapInit = memoryMXBean.getNonHeapMemoryUsage().getInit() < 0 ? 0 : memoryMXBean.getNonHeapMemoryUsage().getInit();
        info.mem.nonHeapMax = memoryMXBean.getNonHeapMemoryUsage().getMax() < 0 ? 0 : memoryMXBean.getNonHeapMemoryUsage().getMax();
        try {
            Class<?> vmClass = Class.forName("sun.misc.VM");
            info.mem.directMemoryMax = (Long) vmClass.getMethod("maxDirectMemory").invoke(null);
        } catch (Throwable t) {
            // ignore
        }
        info.inputArguments = runtimeMXBean.getInputArguments().toArray(new String[runtimeMXBean.getInputArguments().size()]);
        try {
            info.bootClassPath = runtimeMXBean.getBootClassPath();
        } catch (UnsupportedOperationException e) {
            // oracle java 9
            info.bootClassPath = System.getProperty("sun.boot.class.path");
            if (info.bootClassPath == null) {
                // something else
                info.bootClassPath = "<unknown>";
            }
        }
        info.classPath = runtimeMXBean.getClassPath();
        info.systemProperties = Collections.unmodifiableMap(runtimeMXBean.getSystemProperties());

        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        info.gcCollectors = new String[gcMxBeans.size()];
        for (int i = 0; i < gcMxBeans.size(); i++) {
            GarbageCollectorMXBean gcMxBean = gcMxBeans.get(i);
            info.gcCollectors[i] = gcMxBean.getName();
        }

        List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
        info.memoryPools = new String[memoryPoolMXBeans.size()];
        for (int i = 0; i < memoryPoolMXBeans.size(); i++) {
            MemoryPoolMXBean memoryPoolMXBean = memoryPoolMXBeans.get(i);
            info.memoryPools[i] = memoryPoolMXBean.getName();
        }

        INSTANCE = info;
    }

    public static JvmInfo jvmInfo() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new ManagementPermission("monitor"));
            sm.checkPropertyAccess("*");
        }
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

    String[] gcCollectors = Strings.EMPTY_ARRAY;
    String[] memoryPools = Strings.EMPTY_ARRAY;

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

    public int versionAsInteger() {
        try {
            int i = 0;
            String sVersion = "";
            for (; i < version.length(); i++) {
                if (!Character.isDigit(version.charAt(i)) && version.charAt(i) != '.') {
                    break;
                }
                if (version.charAt(i) != '.') {
                    sVersion += version.charAt(i);
                }
            }
            if (i == 0) {
                return -1;
            }
            return Integer.parseInt(sVersion);
        } catch (Exception e) {
            return -1;
        }
    }

    public int versionUpdatePack() {
        try {
            int i = 0;
            String sVersion = "";
            for (; i < version.length(); i++) {
                if (!Character.isDigit(version.charAt(i)) && version.charAt(i) != '.') {
                    break;
                }
                if (version.charAt(i) != '.') {
                    sVersion += version.charAt(i);
                }
            }
            if (i == 0) {
                return -1;
            }
            Integer.parseInt(sVersion);
            int from;
            if (version.charAt(i) == '_') {
                // 1.7.0_4
                from = ++i;
            } else if (version.charAt(i) == '-' && version.charAt(i + 1) == 'u') {
                // 1.7.0-u2-b21
                i = i + 2;
                from = i;
            } else {
                return -1;
            }
            for (; i < version.length(); i++) {
                if (!Character.isDigit(version.charAt(i)) && version.charAt(i) != '.') {
                    break;
                }
            }
            if (from == i) {
                return -1;
            }
            return Integer.parseInt(version.substring(from, i));
        } catch (Exception e) {
            return -1;
        }
    }

    public String getVmName() {
        return this.vmName;
    }

    public String getVmVersion() {
        return this.vmVersion;
    }

    public String getVmVendor() {
        return this.vmVendor;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public Mem getMem() {
        return this.mem;
    }

    public String[] getInputArguments() {
        return this.inputArguments;
    }

    public String getBootClassPath() {
        return this.bootClassPath;
    }

    public String getClassPath() {
        return this.classPath;
    }

    public Map<String, String> getSystemProperties() {
        return this.systemProperties;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.JVM);
        builder.field(Fields.PID, pid);
        builder.field(Fields.VERSION, version);
        builder.field(Fields.VM_NAME, vmName);
        builder.field(Fields.VM_VERSION, vmVersion);
        builder.field(Fields.VM_VENDOR, vmVendor);
        builder.dateValueField(Fields.START_TIME_IN_MILLIS, Fields.START_TIME, startTime);

        builder.startObject(Fields.MEM);
        builder.byteSizeField(Fields.HEAP_INIT_IN_BYTES, Fields.HEAP_INIT, mem.heapInit);
        builder.byteSizeField(Fields.HEAP_MAX_IN_BYTES, Fields.HEAP_MAX, mem.heapMax);
        builder.byteSizeField(Fields.NON_HEAP_INIT_IN_BYTES, Fields.NON_HEAP_INIT, mem.nonHeapInit);
        builder.byteSizeField(Fields.NON_HEAP_MAX_IN_BYTES, Fields.NON_HEAP_MAX, mem.nonHeapMax);
        builder.byteSizeField(Fields.DIRECT_MAX_IN_BYTES, Fields.DIRECT_MAX, mem.directMemoryMax);
        builder.endObject();

        builder.field(Fields.GC_COLLECTORS, gcCollectors);
        builder.field(Fields.MEMORY_POOLS, memoryPools);

        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString JVM = new XContentBuilderString("jvm");
        static final XContentBuilderString PID = new XContentBuilderString("pid");
        static final XContentBuilderString VERSION = new XContentBuilderString("version");
        static final XContentBuilderString VM_NAME = new XContentBuilderString("vm_name");
        static final XContentBuilderString VM_VERSION = new XContentBuilderString("vm_version");
        static final XContentBuilderString VM_VENDOR = new XContentBuilderString("vm_vendor");
        static final XContentBuilderString START_TIME = new XContentBuilderString("start_time");
        static final XContentBuilderString START_TIME_IN_MILLIS = new XContentBuilderString("start_time_in_millis");

        static final XContentBuilderString MEM = new XContentBuilderString("mem");
        static final XContentBuilderString HEAP_INIT = new XContentBuilderString("heap_init");
        static final XContentBuilderString HEAP_INIT_IN_BYTES = new XContentBuilderString("heap_init_in_bytes");
        static final XContentBuilderString HEAP_MAX = new XContentBuilderString("heap_max");
        static final XContentBuilderString HEAP_MAX_IN_BYTES = new XContentBuilderString("heap_max_in_bytes");
        static final XContentBuilderString NON_HEAP_INIT = new XContentBuilderString("non_heap_init");
        static final XContentBuilderString NON_HEAP_INIT_IN_BYTES = new XContentBuilderString("non_heap_init_in_bytes");
        static final XContentBuilderString NON_HEAP_MAX = new XContentBuilderString("non_heap_max");
        static final XContentBuilderString NON_HEAP_MAX_IN_BYTES = new XContentBuilderString("non_heap_max_in_bytes");
        static final XContentBuilderString DIRECT_MAX = new XContentBuilderString("direct_max");
        static final XContentBuilderString DIRECT_MAX_IN_BYTES = new XContentBuilderString("direct_max_in_bytes");
        static final XContentBuilderString GC_COLLECTORS = new XContentBuilderString("gc_collectors");
        static final XContentBuilderString MEMORY_POOLS = new XContentBuilderString("memory_pools");
    }

    public static JvmInfo readJvmInfo(StreamInput in) throws IOException {
        JvmInfo jvmInfo = new JvmInfo();
        jvmInfo.readFrom(in);
        return jvmInfo;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        pid = in.readLong();
        version = in.readString();
        vmName = in.readString();
        vmVersion = in.readString();
        vmVendor = in.readString();
        startTime = in.readLong();
        inputArguments = new String[in.readInt()];
        for (int i = 0; i < inputArguments.length; i++) {
            inputArguments[i] = in.readString();
        }
        bootClassPath = in.readString();
        classPath = in.readString();
        systemProperties = new HashMap<>();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            systemProperties.put(in.readString(), in.readString());
        }
        mem = new Mem();
        mem.readFrom(in);
        gcCollectors = in.readStringArray();
        memoryPools = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(pid);
        out.writeString(version);
        out.writeString(vmName);
        out.writeString(vmVersion);
        out.writeString(vmVendor);
        out.writeLong(startTime);
        out.writeInt(inputArguments.length);
        for (String inputArgument : inputArguments) {
            out.writeString(inputArgument);
        }
        out.writeString(bootClassPath);
        out.writeString(classPath);
        out.writeInt(systemProperties.size());
        for (Map.Entry<String, String> entry : systemProperties.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
        mem.writeTo(out);
        out.writeStringArray(gcCollectors);
        out.writeStringArray(memoryPools);
    }

    public static class Mem implements Streamable {

        long heapInit = 0;
        long heapMax = 0;
        long nonHeapInit = 0;
        long nonHeapMax = 0;
        long directMemoryMax = 0;

        Mem() {
        }

        public ByteSizeValue getHeapInit() {
            return new ByteSizeValue(heapInit);
        }

        public ByteSizeValue getHeapMax() {
            return new ByteSizeValue(heapMax);
        }

        public ByteSizeValue getNonHeapInit() {
            return new ByteSizeValue(nonHeapInit);
        }

        public ByteSizeValue getNonHeapMax() {
            return new ByteSizeValue(nonHeapMax);
        }

        public ByteSizeValue getDirectMemoryMax() {
            return new ByteSizeValue(directMemoryMax);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            heapInit = in.readVLong();
            heapMax = in.readVLong();
            nonHeapInit = in.readVLong();
            nonHeapMax = in.readVLong();
            directMemoryMax = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(heapInit);
            out.writeVLong(heapMax);
            out.writeVLong(nonHeapInit);
            out.writeVLong(nonHeapMax);
            out.writeVLong(directMemoryMax);
        }
    }
}
