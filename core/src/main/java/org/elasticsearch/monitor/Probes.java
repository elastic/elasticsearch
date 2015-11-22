package org.elasticsearch.monitor;

import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;

public class Probes {
    public static short getLoadAndScaleToPercent(Method method, OperatingSystemMXBean osMxBean) {
        if (method != null) {
            try {
                double load = (double) method.invoke(osMxBean);
                if (load >= 0) {
                    return (short) (load * 100);
                }
            } catch (Throwable t) {
                return -1;
            }
        }
        return -1;
    }
}
