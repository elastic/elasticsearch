package org.elasticsearch.gradle.vagrant

/**
 * Describes the installation of an external dependency of the build environment,
 * like Vagrant, or VirtualBox. Provides facilities for determining if the command
 * is installed, if it is the correct version, which version was found, and any
 * errors that were encountered when checking for the installation.
 */
class Installation {
    public static Installation supported(String versionString) {
        return new Installation(true, true, versionString, null)
    }

    public static Installation unsupported(Throwable error) {
        return new Installation(true, false, "Unsupported", error)
    }

    public static Installation notInstalled(Throwable error) {
        return new Installation(false, false, "NotInstalled", error)
    }

    boolean installed
    boolean versionSupported
    String versionString
    Throwable error

    private Installation(boolean installed, boolean versionSupported, String versionString, Throwable error) {
        this.installed = installed
        this.versionSupported = versionSupported
        this.versionString = versionString
        this.error = error
    }

    /**
     * Checks to make sure that the Installation is correct, and if not,
     * throws an exception with the reason why.
     */
    void verify() {
        if ((installed && versionSupported) == false) {
            throw error
        }
    }

    boolean getSupported() {
        return installed && versionSupported
    }
}
