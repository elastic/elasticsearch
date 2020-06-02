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

import org.apache.lucene.util.Constants;
import org.elasticsearch.Version;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.ReportingService;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ManagementPermission;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.PlatformManagedObject;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JvmInfo implements ReportingService.Info {

    private static JvmInfo INSTANCE;

    static {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

        long heapInit = memoryMXBean.getHeapMemoryUsage().getInit() < 0 ? 0 : memoryMXBean.getHeapMemoryUsage().getInit();
        long heapMax = memoryMXBean.getHeapMemoryUsage().getMax() < 0 ? 0 : memoryMXBean.getHeapMemoryUsage().getMax();
        long nonHeapInit = memoryMXBean.getNonHeapMemoryUsage().getInit() < 0 ? 0 : memoryMXBean.getNonHeapMemoryUsage().getInit();
        long nonHeapMax = memoryMXBean.getNonHeapMemoryUsage().getMax() < 0 ? 0 : memoryMXBean.getNonHeapMemoryUsage().getMax();
        long directMemoryMax = 0;
        try {
            Class<?> vmClass = Class.forName("sun.misc.VM");
            directMemoryMax = (Long) vmClass.getMethod("maxDirectMemory").invoke(null);
        } catch (Exception t) {
            // ignore
        }
        String[] inputArguments = runtimeMXBean.getInputArguments().toArray(new String[runtimeMXBean.getInputArguments().size()]);
        Mem mem = new Mem(heapInit, heapMax, nonHeapInit, nonHeapMax, directMemoryMax);

        String bootClassPath;
        try {
            bootClassPath = runtimeMXBean.getBootClassPath();
        } catch (UnsupportedOperationException e) {
            // oracle java 9
            bootClassPath = System.getProperty("sun.boot.class.path");
            if (bootClassPath == null) {
                // something else
                bootClassPath = "<unknown>";
            }
        }
        String classPath = runtimeMXBean.getClassPath();
        Map<String, String> systemProperties = Collections.unmodifiableMap(runtimeMXBean.getSystemProperties());

        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        String[] gcCollectors = new String[gcMxBeans.size()];
        for (int i = 0; i < gcMxBeans.size(); i++) {
            GarbageCollectorMXBean gcMxBean = gcMxBeans.get(i);
            gcCollectors[i] = gcMxBean.getName();
        }

        List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
        String[] memoryPools = new String[memoryPoolMXBeans.size()];
        for (int i = 0; i < memoryPoolMXBeans.size(); i++) {
            MemoryPoolMXBean memoryPoolMXBean = memoryPoolMXBeans.get(i);
            memoryPools[i] = memoryPoolMXBean.getName();
        }

        String onError = null;
        String onOutOfMemoryError = null;
        String useCompressedOops = "unknown";
        String useG1GC = "unknown";
        String useSerialGC = "unknown";
        long configuredInitialHeapSize = -1;
        long configuredMaxHeapSize = -1;
        try {
            @SuppressWarnings("unchecked") Class<? extends PlatformManagedObject> clazz =
                    (Class<? extends PlatformManagedObject>)Class.forName("com.sun.management.HotSpotDiagnosticMXBean");
            Class<?> vmOptionClazz = Class.forName("com.sun.management.VMOption");
            PlatformManagedObject hotSpotDiagnosticMXBean = ManagementFactory.getPlatformMXBean(clazz);
            Method vmOptionMethod = clazz.getMethod("getVMOption", String.class);
            Method valueMethod = vmOptionClazz.getMethod("getValue");

            try {
                Object onErrorObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "OnError");
                onError = (String) valueMethod.invoke(onErrorObject);
            } catch (Exception ignored) {
            }

            try {
                Object onOutOfMemoryErrorObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "OnOutOfMemoryError");
                onOutOfMemoryError = (String) valueMethod.invoke(onOutOfMemoryErrorObject);
            } catch (Exception ignored) {
            }

            try {
                Object useCompressedOopsVmOptionObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "UseCompressedOops");
                useCompressedOops = (String) valueMethod.invoke(useCompressedOopsVmOptionObject);
            } catch (Exception ignored) {
            }

            try {
                Object useG1GCVmOptionObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "UseG1GC");
                useG1GC = (String) valueMethod.invoke(useG1GCVmOptionObject);
            } catch (Exception ignored) {
            }

            try {
                Object initialHeapSizeVmOptionObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "InitialHeapSize");
                configuredInitialHeapSize = Long.parseLong((String) valueMethod.invoke(initialHeapSizeVmOptionObject));
            } catch (Exception ignored) {
            }

            try {
                Object maxHeapSizeVmOptionObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "MaxHeapSize");
                configuredMaxHeapSize = Long.parseLong((String) valueMethod.invoke(maxHeapSizeVmOptionObject));
            } catch (Exception ignored) {
            }

            try {
                Object useSerialGCVmOptionObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "UseSerialGC");
                useSerialGC = (String) valueMethod.invoke(useSerialGCVmOptionObject);
            } catch (Exception ignored) {
            }

        } catch (Exception ignored) {

        }

        final boolean bundledJdk = Booleans.parseBoolean(System.getProperty("es.bundled_jdk", Boolean.FALSE.toString()));
        final Boolean usingBundledJdk = bundledJdk ? usingBundledJdk() : null;

        INSTANCE = new JvmInfo(
                ProcessHandle.current().pid(),
                System.getProperty("java.version"),
                runtimeMXBean.getVmName(),
                runtimeMXBean.getVmVersion(),
                runtimeMXBean.getVmVendor(),
                bundledJdk,
                usingBundledJdk,
                runtimeMXBean.getStartTime(),
                configuredInitialHeapSize,
                configuredMaxHeapSize,
                mem,
                inputArguments,
                bootClassPath,
                classPath,
                systemProperties,
                gcCollectors,
                memoryPools,
                onError,
                onOutOfMemoryError,
                useCompressedOops,
                useG1GC,
                useSerialGC);
    }

    @SuppressForbidden(reason = "PathUtils#get")
    private static boolean usingBundledJdk() {
        /*
         * We are using the bundled JDK if java.home is the jdk sub-directory of our working directory. This is because we always set
         * the working directory of Elasticsearch to home, and the bundled JDK is in the jdk sub-directory there.
         */
        final String javaHome = System.getProperty("java.home");
        final String userDir = System.getProperty("user.dir");
        if (Constants.MAC_OS_X) {
            return PathUtils.get(javaHome).equals(PathUtils.get(userDir).resolve("jdk/Contents/Home").toAbsolutePath());
        } else {
            return PathUtils.get(javaHome).equals(PathUtils.get(userDir).resolve("jdk").toAbsolutePath());
        }
    }

    public static JvmInfo jvmInfo() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new ManagementPermission("monitor"));
            sm.checkPropertyAccess("*");
        }
        return INSTANCE;
    }

    private final long pid;
    private final String version;
    private final String vmName;
    private final String vmVersion;
    private final String vmVendor;
    private final boolean bundledJdk;
    private final Boolean usingBundledJdk;
    private final long startTime;
    private final long configuredInitialHeapSize;
    private final long configuredMaxHeapSize;
    private final Mem mem;
    private final String[] inputArguments;
    private final String bootClassPath;
    private final String classPath;
    private final Map<String, String> systemProperties;
    private final String[] gcCollectors;
    private final String[] memoryPools;
    private final String onError;
    private final String onOutOfMemoryError;
    private final String useCompressedOops;
    private final String useG1GC;
    private final String useSerialGC;

    private JvmInfo(long pid, String version, String vmName, String vmVersion, String vmVendor, boolean bundledJdk, Boolean usingBundledJdk,
                    long startTime, long configuredInitialHeapSize, long configuredMaxHeapSize, Mem mem, String[] inputArguments,
                    String bootClassPath, String classPath, Map<String, String> systemProperties, String[] gcCollectors,
                    String[] memoryPools, String onError, String onOutOfMemoryError, String useCompressedOops, String useG1GC,
                    String useSerialGC) {
        this.pid = pid;
        this.version = version;
        this.vmName = vmName;
        this.vmVersion = vmVersion;
        this.vmVendor = vmVendor;
        this.bundledJdk = bundledJdk;
        this.usingBundledJdk = usingBundledJdk;
        this.startTime = startTime;
        this.configuredInitialHeapSize = configuredInitialHeapSize;
        this.configuredMaxHeapSize = configuredMaxHeapSize;
        this.mem = mem;
        this.inputArguments = inputArguments;
        this.bootClassPath = bootClassPath;
        this.classPath = classPath;
        this.systemProperties = systemProperties;
        this.gcCollectors = gcCollectors;
        this.memoryPools = memoryPools;
        this.onError = onError;
        this.onOutOfMemoryError = onOutOfMemoryError;
        this.useCompressedOops = useCompressedOops;
        this.useG1GC = useG1GC;
        this.useSerialGC = useSerialGC;
    }

    public JvmInfo(StreamInput in) throws IOException {
        pid = in.readLong();
        version = in.readString();
        vmName = in.readString();
        vmVersion = in.readString();
        vmVendor = in.readString();
        if (in.getVersion().onOrAfter(Version.V_7_0_0)) {
            bundledJdk = in.readBoolean();
            usingBundledJdk = in.readOptionalBoolean();
        } else {
            bundledJdk = false;
            usingBundledJdk = null;
        }
        startTime = in.readLong();
        inputArguments = new String[in.readInt()];
        for (int i = 0; i < inputArguments.length; i++) {
            inputArguments[i] = in.readString();
        }
        bootClassPath = in.readString();
        classPath = in.readString();
        systemProperties = in.readMap(StreamInput::readString, StreamInput::readString);
        mem = new Mem(in);
        gcCollectors = in.readStringArray();
        memoryPools = in.readStringArray();
        useCompressedOops = in.readString();
        //the following members are only used locally for bootstrap checks, never serialized nor printed out
        this.configuredMaxHeapSize = -1;
        this.configuredInitialHeapSize = -1;
        this.onError = null;
        this.onOutOfMemoryError = null;
        this.useG1GC = "unknown";
        this.useSerialGC = "unknown";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(pid);
        out.writeString(version);
        out.writeString(vmName);
        out.writeString(vmVersion);
        out.writeString(vmVendor);
        if (out.getVersion().onOrAfter(Version.V_7_0_0)) {
            out.writeBoolean(bundledJdk);
            out.writeOptionalBoolean(usingBundledJdk);
        }
        out.writeLong(startTime);
        out.writeInt(inputArguments.length);
        for (String inputArgument : inputArguments) {
            out.writeString(inputArgument);
        }
        out.writeString(bootClassPath);
        out.writeString(classPath);
        out.writeVInt(this.systemProperties.size());
        for (Map.Entry<String, String> entry : systemProperties.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
        mem.writeTo(out);
        out.writeStringArray(gcCollectors);
        out.writeStringArray(memoryPools);
        out.writeString(useCompressedOops);
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
            StringBuilder sVersion = new StringBuilder();
            for (; i < version.length(); i++) {
                if (!Character.isDigit(version.charAt(i)) && version.charAt(i) != '.') {
                    break;
                }
                if (version.charAt(i) != '.') {
                    sVersion.append(version.charAt(i));
                }
            }
            if (i == 0) {
                return -1;
            }
            return Integer.parseInt(sVersion.toString());
        } catch (Exception e) {
            return -1;
        }
    }

    public int versionUpdatePack() {
        try {
            int i = 0;
            StringBuilder sVersion = new StringBuilder();
            for (; i < version.length(); i++) {
                if (!Character.isDigit(version.charAt(i)) && version.charAt(i) != '.') {
                    break;
                }
                if (version.charAt(i) != '.') {
                    sVersion.append(version.charAt(i));
                }
            }
            if (i == 0) {
                return -1;
            }
            Integer.parseInt(sVersion.toString());
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

    public boolean getBundledJdk() {
        return bundledJdk;
    }

    public Boolean getUsingBundledJdk() {
        return usingBundledJdk;
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

    public long getConfiguredInitialHeapSize() {
        return configuredInitialHeapSize;
    }

    public long getConfiguredMaxHeapSize() {
        return configuredMaxHeapSize;
    }

    public String onError() {
        return onError;
    }

    public String onOutOfMemoryError() {
        return onOutOfMemoryError;
    }

    /**
     * The value of the JVM flag UseCompressedOops, if available otherwise
     * "unknown". The value "unknown" indicates that an attempt was
     * made to obtain the value of the flag on this JVM and the attempt
     * failed.
     *
     * @return the value of the JVM flag UseCompressedOops or "unknown"
     */
    public String useCompressedOops() {
        return this.useCompressedOops;
    }

    public String useG1GC() {
        return this.useG1GC;
    }

    public String useSerialGC() {
        return this.useSerialGC;
    }

    public String[] getGcCollectors() {
        return gcCollectors;
    }

    public String[] getMemoryPools() {
        return memoryPools;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.JVM);
        builder.field(Fields.PID, pid);
        builder.field(Fields.VERSION, version);
        builder.field(Fields.VM_NAME, vmName);
        builder.field(Fields.VM_VERSION, vmVersion);
        builder.field(Fields.VM_VENDOR, vmVendor);
        builder.field(Fields.BUNDLED_JDK, bundledJdk);
        builder.field(Fields.USING_BUNDLED_JDK, usingBundledJdk);
        builder.timeField(Fields.START_TIME_IN_MILLIS, Fields.START_TIME, startTime);

        builder.startObject(Fields.MEM);
        builder.humanReadableField(Fields.HEAP_INIT_IN_BYTES, Fields.HEAP_INIT, new ByteSizeValue(mem.heapInit));
        builder.humanReadableField(Fields.HEAP_MAX_IN_BYTES, Fields.HEAP_MAX, new ByteSizeValue(mem.heapMax));
        builder.humanReadableField(Fields.NON_HEAP_INIT_IN_BYTES, Fields.NON_HEAP_INIT, new ByteSizeValue(mem.nonHeapInit));
        builder.humanReadableField(Fields.NON_HEAP_MAX_IN_BYTES, Fields.NON_HEAP_MAX, new ByteSizeValue(mem.nonHeapMax));
        builder.humanReadableField(Fields.DIRECT_MAX_IN_BYTES, Fields.DIRECT_MAX, new ByteSizeValue(mem.directMemoryMax));
        builder.endObject();

        builder.array(Fields.GC_COLLECTORS, gcCollectors);
        builder.array(Fields.MEMORY_POOLS, memoryPools);

        builder.field(Fields.USING_COMPRESSED_OOPS, useCompressedOops);

        builder.field(Fields.INPUT_ARGUMENTS, inputArguments);

        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String JVM = "jvm";
        static final String PID = "pid";
        static final String VERSION = "version";
        static final String VM_NAME = "vm_name";
        static final String VM_VERSION = "vm_version";
        static final String VM_VENDOR = "vm_vendor";
        static final String BUNDLED_JDK = "bundled_jdk";
        static final String USING_BUNDLED_JDK = "using_bundled_jdk";
        static final String START_TIME = "start_time";
        static final String START_TIME_IN_MILLIS = "start_time_in_millis";

        static final String MEM = "mem";
        static final String HEAP_INIT = "heap_init";
        static final String HEAP_INIT_IN_BYTES = "heap_init_in_bytes";
        static final String HEAP_MAX = "heap_max";
        static final String HEAP_MAX_IN_BYTES = "heap_max_in_bytes";
        static final String NON_HEAP_INIT = "non_heap_init";
        static final String NON_HEAP_INIT_IN_BYTES = "non_heap_init_in_bytes";
        static final String NON_HEAP_MAX = "non_heap_max";
        static final String NON_HEAP_MAX_IN_BYTES = "non_heap_max_in_bytes";
        static final String DIRECT_MAX = "direct_max";
        static final String DIRECT_MAX_IN_BYTES = "direct_max_in_bytes";
        static final String GC_COLLECTORS = "gc_collectors";
        static final String MEMORY_POOLS = "memory_pools";
        static final String USING_COMPRESSED_OOPS = "using_compressed_ordinary_object_pointers";
        static final String INPUT_ARGUMENTS = "input_arguments";
    }

    public static class Mem implements Writeable {

        private final long heapInit;
        private final long heapMax;
        private final long nonHeapInit;
        private final long nonHeapMax;
        private final long directMemoryMax;

        public Mem(long heapInit, long heapMax, long nonHeapInit, long nonHeapMax, long directMemoryMax) {
            this.heapInit = heapInit;
            this.heapMax = heapMax;
            this.nonHeapInit = nonHeapInit;
            this.nonHeapMax = nonHeapMax;
            this.directMemoryMax = directMemoryMax;
        }

        public Mem(StreamInput in) throws IOException {
            this.heapInit = in.readVLong();
            this.heapMax = in.readVLong();
            this.nonHeapInit = in.readVLong();
            this.nonHeapMax = in.readVLong();
            this.directMemoryMax = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(heapInit);
            out.writeVLong(heapMax);
            out.writeVLong(nonHeapInit);
            out.writeVLong(nonHeapMax);
            out.writeVLong(directMemoryMax);
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
    }
}
