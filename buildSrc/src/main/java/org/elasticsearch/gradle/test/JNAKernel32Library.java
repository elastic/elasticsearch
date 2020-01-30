package org.elasticsearch.gradle.test;

import com.sun.jna.Native;
import com.sun.jna.WString;
import org.apache.tools.ant.taskdefs.condition.Os;

public class JNAKernel32Library {

    private static final class Holder {
        private static final JNAKernel32Library instance = new JNAKernel32Library();
    }

    static JNAKernel32Library getInstance() {
        return Holder.instance;
    }

    private JNAKernel32Library() {
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            Native.register("kernel32");
        }
    }

    native int GetShortPathNameW(WString lpszLongPath, char[] lpszShortPath, int cchBuffer);

}
