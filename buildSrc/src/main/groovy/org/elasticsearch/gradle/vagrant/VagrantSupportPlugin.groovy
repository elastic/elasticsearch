package org.elasticsearch.gradle.vagrant

import org.gradle.api.GradleException
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.process.ExecResult

/**
 * Global configuration for if Vagrant tasks are supported in this
 * build environment.
 */
class VagrantSupportPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        if (project.rootProject.ext.has('vagrantEnvChecksDone') == false) {
            Installation vagrantInstallation = getVagrantInstallation(project)
            Installation virtualBoxInstallation = getVirtualBoxInstallation(project)

            project.rootProject.ext.vagrantInstallation = vagrantInstallation
            project.rootProject.ext.virtualBoxInstallation = virtualBoxInstallation
            project.rootProject.ext.vagrantSupported = vagrantInstallation.supported && virtualBoxInstallation.supported
            project.rootProject.ext.vagrantEnvChecksDone = true
        }

        // Finding that HOME needs to be set when performing vagrant updates
        String homeLocation = System.getenv("HOME")
        if (project.rootProject.ext.vagrantSupported && homeLocation == null) {
            throw new GradleException("Could not locate \$HOME environment variable. Vagrant is enabled " +
                    "and requires \$HOME to be set to function properly.")
        }

        project.ext.vagrantInstallation = project.rootProject.ext.vagrantInstallation
        project.ext.virtualBoxInstallation = project.rootProject.ext.virtualBoxInstallation
        project.ext.vagrantSupported = project.rootProject.ext.vagrantSupported

        addVerifyInstallationTasks(project)
    }

    private Installation getVagrantInstallation(Project project) {
        // Only do secure fixture support if the regular fixture is supported,
        // and if vagrant is installed. The ignoreExitValue on exec only matters
        // in cases where the command can be found and successfully started. In
        // situations where the vagrant command isn't able to be started at all
        // (it's not installed) then Gradle still throws ExecException.
        try {
            ByteArrayOutputStream pipe = new ByteArrayOutputStream()
            ExecResult runResult = project.exec {
                commandLine 'vagrant', '--version'
                standardOutput pipe
                ignoreExitValue true
            }
            String version = pipe.toString().trim()
            if (runResult.exitValue == 0) {
                if (version ==~ /Vagrant 1\.(8\.[6-9]|9\.[0-9])+/) {
                    return Installation.supported(version)
                } else {
                    return Installation.unsupported(new InvalidUserDataException(
                            "Illegal version of vagrant [${version}]. Need [Vagrant 1.8.6+]"))
                }
            } else {
                Installation.unsupported(new InvalidUserDataException(
                        "Could not read installed vagrant version:\n" + version))
            }
        } catch (org.gradle.process.internal.ExecException e) {
            // Swallow error. Vagrant isn't installed. Let users of plugin decide to throw or not.
            return Installation.notInstalled(new InvalidUserDataException("Could not find vagrant: " + e.message))
        }
    }

    private Installation getVirtualBoxInstallation(Project project) {
        // Also check to see if virtualbox is installed. We need both vagrant
        // and virtualbox to be installed for the secure environment to be
        // enabled.
        try {
            ByteArrayOutputStream pipe = new ByteArrayOutputStream()
            ExecResult runResult = project.exec {
                commandLine 'vboxmanage', '--version'
                standardOutput = pipe
                ignoreExitValue true
            }
            String version = pipe.toString().trim()
            if (runResult.exitValue == 0) {
                try {
                    String[] versions = version.split('\\.')
                    int major = Integer.parseInt(versions[0])
                    int minor = Integer.parseInt(versions[1])
                    if ((major < 5) || (major == 5 && minor < 1)) {
                        return Installation.unsupported(new InvalidUserDataException(
                                "Illegal version of virtualbox [${version}]. Need [5.1+]"))
                    } else {
                        return Installation.supported(version)
                    }
                } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                    return Installation.unsupported(new InvalidUserDataException(
                            "Unable to parse version of virtualbox [${version}]. Required [5.1+]", e))
                }
            } else {
                return Installation.unsupported(new InvalidUserDataException(
                        "Could not read installed virtualbox version:\n" + version))
            }
        } catch (org.gradle.process.internal.ExecException e) {
            // Swallow error. VirtualBox isn't installed.
            return Installation.notInstalled(new InvalidUserDataException("Could not find virtualbox: " + e.message))
        }
    }

    private void addVerifyInstallationTasks(Project project) {
        createCheckVagrantVersionTask(project)
        createCheckVirtualBoxVersionTask(project)
    }

    private void createCheckVagrantVersionTask(Project project) {
        project.tasks.create('vagrantCheckVersion') {
            description 'Check the Vagrant version'
            group 'Verification'
            doLast {
                project.vagrantInstallation.verify
            }
        }
    }

    private void createCheckVirtualBoxVersionTask(Project project) {
        project.tasks.create('virtualboxCheckVersion') {
            description 'Check the Virtualbox version'
            group 'Verification'
            doLast {
                project.virtualBoxInstallation.verify
            }
        }
    }
}
