package org.elasticsearch.gradle.vagrant

import org.gradle.api.GradleException
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.process.ExecResult
import org.gradle.process.internal.ExecException

/**
 * Global configuration for if Vagrant tasks are supported in this
 * build environment.
 */
class VagrantSupportPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        if (project.rootProject.ext.has('vagrantEnvChecksDone') == false) {
            Map vagrantInstallation = getVagrantInstallation(project)
            Map virtualBoxInstallation = getVirtualBoxInstallation(project)

            project.rootProject.ext.vagrantInstallation = vagrantInstallation
            project.rootProject.ext.virtualBoxInstallation = virtualBoxInstallation
            project.rootProject.ext.vagrantSupported = vagrantInstallation.supported && virtualBoxInstallation.supported
            project.rootProject.ext.vagrantEnvChecksDone = true

            // Finding that HOME needs to be set when performing vagrant updates
            String homeLocation = System.getenv("HOME")
            if (project.rootProject.ext.vagrantSupported && homeLocation == null) {
                throw new GradleException("Could not locate \$HOME environment variable. Vagrant is enabled " +
                        "and requires \$HOME to be set to function properly.")
            }
        }

        addVerifyInstallationTasks(project)
    }

    private Map getVagrantInstallation(Project project) {
        try {
            ByteArrayOutputStream pipe = new ByteArrayOutputStream()
            ExecResult runResult = project.exec {
                commandLine 'vagrant', '--version'
                standardOutput pipe
                ignoreExitValue true
            }
            String version = pipe.toString().trim()
            if (runResult.exitValue == 0) {
                if (version ==~ /Vagrant 1\.(8\.[6-9]|9\.[0-9])+/ || version ==~ /Vagrant 2\.[0-9]+\.[0-9]+/) {
                    return [ 'supported' : true ]
                } else {
                    return [ 'supported' : false,
                             'info' : "Illegal version of vagrant [${version}]. Need [Vagrant 1.8.6+]" ]
                }
            } else {
                return [ 'supported' : false,
                         'info' : "Could not read installed vagrant version:\n" + version ]
            }
        } catch (ExecException e) {
            // Exec still throws this if it cannot find the command, regardless if ignoreExitValue is set.
            // Swallow error. Vagrant isn't installed. Don't halt the build here.
            return [ 'supported' : false, 'info' : "Could not find vagrant: " + e.message ]
        }
    }

    private Map getVirtualBoxInstallation(Project project) {
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
                        return [ 'supported' : false,
                                 'info' : "Illegal version of virtualbox [${version}]. Need [5.1+]" ]
                    } else {
                        return [ 'supported' : true ]
                    }
                } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                    return [ 'supported' : false,
                             'info' : "Unable to parse version of virtualbox [${version}]. Required [5.1+]" ]
                }
            } else {
                return [ 'supported': false, 'info': "Could not read installed virtualbox version:\n" + version ]
            }
        } catch (ExecException e) {
            // Exec still throws this if it cannot find the command, regardless if ignoreExitValue is set.
            // Swallow error. VirtualBox isn't installed. Don't halt the build here.
            return [ 'supported' : false, 'info' : "Could not find virtualbox: " + e.message ]
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
                if (project.rootProject.vagrantInstallation.supported == false) {
                    throw new InvalidUserDataException(project.rootProject.vagrantInstallation.info)
                }
            }
        }
    }

    private void createCheckVirtualBoxVersionTask(Project project) {
        project.tasks.create('virtualboxCheckVersion') {
            description 'Check the Virtualbox version'
            group 'Verification'
            doLast {
                if (project.rootProject.virtualBoxInstallation.supported == false) {
                    throw new InvalidUserDataException(project.rootProject.virtualBoxInstallation.info)
                }
            }
        }
    }
}
