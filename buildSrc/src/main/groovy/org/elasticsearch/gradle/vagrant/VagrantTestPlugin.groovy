package org.elasticsearch.gradle.vagrant

import org.elasticsearch.gradle.FileContentsTask
import org.gradle.api.*
import org.gradle.api.artifacts.dsl.RepositoryHandler
import org.gradle.api.execution.TaskExecutionAdapter
import org.gradle.api.internal.artifacts.dependencies.DefaultProjectDependency
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.TaskState

class VagrantTestPlugin implements Plugin<Project> {

    /** All Linux boxes **/
    static List<String> LINUX_BOXES = [
            'centos-6',
            'centos-7',
            'debian-8',
            'debian-9',
            'fedora-26',
            'fedora-27',
            'oel-6',
            'oel-7',
            'opensuse-42',
            'sles-12',
            'ubuntu-1404',
            'ubuntu-1604'
    ]

    /** Windows boxes we have images for and can build with **/
    static Map<String, String> AVAILABLE_WINDOWS_BOXES = [:]

    /** All available boxes **/
    static List<String> BOXES

    /** Boxes used when sampling the tests **/
    static List<String> SAMPLE = [
            'centos-7',
            'ubuntu-1404',
    ]

    /** All onboarded archives by default, available for Bats tests even if not used **/
    static List<String> DISTRIBUTION_ARCHIVES = ['tar', 'rpm', 'deb']

    /** Packages onboarded for upgrade tests **/
    static List<String> UPGRADE_FROM_ARCHIVES = ['rpm', 'deb']

    static final PACKAGING_CONFIGURATION = 'packaging'

    private static final BATS = 'bats'
    private static final String BATS_TEST_COMMAND ="cd \$BATS_ARCHIVES && sudo bats --tap \$BATS_TESTS/*.$BATS"

    @Override
    void apply(Project project) {

        collectAvailableBoxes(project)

        // Creates the Vagrant extension for the project
        project.extensions.create('esvagrant', VagrantPropertiesExtension)
        List<String> boxesToRun = listSelectedVagrantBoxes(project)
        assert BOXES.containsAll(boxesToRun)
        project.extensions.esvagrant.boxes = boxesToRun

        project.extensions.esvagrant.vagrantEnvVars = collectVagrantEnvVars(project)

        // Add required repositories for Bats tests
        configureBatsRepositories(project)

        // Creates custom configurations for Bats testing files (and associated scripts and archives)
        createPackagingConfiguration(project)

        // Creates all the main Vagrant tasks
        createVagrantTasks(project)

        if (project.extensions.esvagrant.boxes == null || project.extensions.esvagrant.boxes.size() == 0) {
            throw new InvalidUserDataException('Vagrant boxes cannot be null or empty for esvagrant')
        }

        for (String box : project.extensions.esvagrant.boxes) {
            if (BOXES.contains(box) == false) {
                throw new InvalidUserDataException("Vagrant box [${box}] not found, available virtual machines are ${BOXES}")
            }
        }

        // Creates all tasks related to the Vagrant boxes
        createVagrantBoxesTasks(project)
    }

    private static void collectAvailableBoxes(Project project) {
        String windows_2012r2_box = project.properties.get('vagrant.windows.2012r2.box', null)
        if (windows_2012r2_box != null && windows_2012r2_box.isEmpty() == false) {
            AVAILABLE_WINDOWS_BOXES['windows-2012r2'] = windows_2012r2_box
        }

        String windows_2016_box = project.properties.get('vagrant.windows.2016.box', null)
        if (windows_2016_box != null && windows_2016_box.isEmpty() == false) {
            AVAILABLE_WINDOWS_BOXES['windows-2016'] = windows_2016_box
        }

        BOXES = LINUX_BOXES + AVAILABLE_WINDOWS_BOXES.keySet()
        project.ext.boxes = BOXES
    }

    private static Map<String, String> collectVagrantEnvVars(Project project) {
        /*
         * We always use the main project.rootDir as Vagrant's current working directory (VAGRANT_CWD)
         * so that boxes are not duplicated for every Gradle project that use this VagrantTestPlugin.
         */
        def vagrantEnvVars = [
                'VAGRANT_CWD'           : "${project.rootDir.absolutePath}",
                'VAGRANT_VAGRANTFILE'   : 'Vagrantfile',
                'VAGRANT_PROJECT_DIR'   : "${project.projectDir.absolutePath}"
        ]
        if ('windows-2012r2' in AVAILABLE_WINDOWS_BOXES) {
            vagrantEnvVars['VAGRANT_WINDOWS_2012R2_BOX'] = AVAILABLE_WINDOWS_BOXES['windows-2012r2']
        }
        if ('windows-2016' in AVAILABLE_WINDOWS_BOXES) {
            vagrantEnvVars['VAGRANT_WINDOWS_2016_BOX'] = AVAILABLE_WINDOWS_BOXES['windows-2016']
        }

        return vagrantEnvVars
    }

    private static List<String> listSelectedVagrantBoxes(Project project) {
        String vagrantBoxes = project.getProperties().get('vagrant.boxes', 'sample')
        switch (vagrantBoxes) {
            case 'sample':
                return SAMPLE
            case 'linux-all':
                return LINUX_BOXES
            case 'windows-all':
                return AVAILABLE_WINDOWS_BOXES.keySet().toList()
            case 'all':
                return BOXES
            default:
                return vagrantBoxes.split(',')
        }
    }

    private static void configureBatsRepositories(Project project) {
        RepositoryHandler repos = project.repositories

        // Try maven central first, it'll have releases before 5.0.0
        repos.mavenCentral()

        /* Setup a repository that tries to download from
          https://artifacts.elastic.co/downloads/elasticsearch/[module]-[revision].[ext]
          which should work for 5.0.0+. This isn't a real ivy repository but gradle
          is fine with that */
        repos.ivy {
            artifactPattern "https://artifacts.elastic.co/downloads/elasticsearch/[module]-[revision].[ext]"
        }
    }

    private static void createPackagingConfiguration(Project project) {
        project.configurations.create(PACKAGING_CONFIGURATION)

        String upgradeFromVersion = System.getProperty("tests.packaging.upgradeVersion")
        if (upgradeFromVersion == null) {
            String firstPartOfSeed = project.rootProject.testSeed.tokenize(':').get(0)
            final long seed = Long.parseUnsignedLong(firstPartOfSeed, 16)
            final def indexCompatVersions = project.versionCollection.versionsIndexCompatibleWithCurrent
            upgradeFromVersion = indexCompatVersions[new Random(seed).nextInt(indexCompatVersions.size())]
        }

        DISTRIBUTION_ARCHIVES.each {
            // Adds a dependency for the current version
            project.dependencies.add(PACKAGING_CONFIGURATION,
                    project.dependencies.project(path: ":distribution:${it}", configuration: 'archives'))
        }

        UPGRADE_FROM_ARCHIVES.each {
            // The version of elasticsearch that we upgrade *from*
            project.dependencies.add(PACKAGING_CONFIGURATION,
                    "org.elasticsearch.distribution.${it}:elasticsearch:${upgradeFromVersion}@${it}")
        }

        project.extensions.esvagrant.upgradeFromVersion = upgradeFromVersion
    }

    private static void createCleanTask(Project project) {
        project.tasks.create('clean', Delete.class) {
            description 'Clean the project build directory'
            group 'Build'
            delete project.buildDir
        }
    }

    private static void createStopTask(Project project) {
        project.tasks.create('stop') {
            description 'Stop any tasks from tests that still may be running'
            group 'Verification'
        }
    }

    private static void createSmokeTestTask(Project project) {
        project.tasks.create('vagrantSmokeTest') {
            description 'Smoke test the specified vagrant boxes'
            group 'Verification'
        }
    }

    private static void createPrepareVagrantTestEnvTask(Project project) {
        File packagingDir = new File("${project.buildDir}/${PACKAGING_CONFIGURATION}")
        File batsDir = new File(packagingDir, BATS)

        Task createPackagingDirsTask = project.tasks.create('createBatsDirs')
        createPackagingDirsTask.outputs.dir batsDir
        createPackagingDirsTask.doLast {
            batsDir.mkdirs()
        }

        Copy copyArchives = project.tasks.create('copyPackagingArchives', Copy) {
            dependsOn createPackagingDirsTask
            into "${packagingDir}/archives"
            from project.configurations[PACKAGING_CONFIGURATION]
        }

        Copy copyBatsTests = project.tasks.create('copyBatsTests', Copy) {
            dependsOn createPackagingDirsTask
            into "${batsDir}/tests"
            from {
                "${project.extensions.esvagrant.batsDir}/tests"
            }
        }

        Copy copyBatsUtils = project.tasks.create('copyBatsUtils', Copy) {
            dependsOn createPackagingDirsTask
            into "${batsDir}/utils"
            from {
                "${project.extensions.esvagrant.batsDir}/utils"
            }
        }

        // Now we iterate over dependencies of the bats configuration. When a project dependency is found,
        // we bring back its own archives, test files or test utils.
        project.afterEvaluate {
            project.configurations.packaging.dependencies.findAll {it.targetConfiguration == PACKAGING_CONFIGURATION }.each { d ->
                if (d instanceof DefaultProjectDependency) {
                    DefaultProjectDependency externalBatsDependency = (DefaultProjectDependency) d
                    Project externalBatsProject = externalBatsDependency.dependencyProject
                    String externalBatsDir = externalBatsProject.extensions.esvagrant.batsDir

                    if (project.extensions.esvagrant.inheritTests) {
                        copyBatsTests.from(externalBatsProject.files("${externalBatsDir}/tests"))
                    }
                    if (project.extensions.esvagrant.inheritTestUtils) {
                        copyBatsUtils.from(externalBatsProject.files("${externalBatsDir}/utils"))
                    }
                }
            }
        }

        Task createVersionFile = project.tasks.create('createVersionFile', FileContentsTask) {
            dependsOn createPackagingDirsTask
            file "${packagingDir}/archives/version"
            contents project.version
        }

        Task createUpgradeFromFile = project.tasks.create('createUpgradeFromFile', FileContentsTask) {
            dependsOn createPackagingDirsTask
            file "${packagingDir}/archives/upgrade_from_version"
            contents project.extensions.esvagrant.upgradeFromVersion
        }

        Task vagrantSetUpTask = project.tasks.create('setupPackaging')
        vagrantSetUpTask.dependsOn 'vagrantCheckVersion'
        vagrantSetUpTask.dependsOn copyBatsTests, copyBatsUtils, copyArchives, createVersionFile, createUpgradeFromFile
    }

    private static void createPackagingTestTask(Project project) {
        project.tasks.create('packagingTest') {
            group 'Verification'
            description "Tests yum/apt packages using vagrant and bats.\n" +
                    "    Specify the vagrant boxes to test using the gradle property 'vagrant.boxes'.\n" +
                    "    'sample' can be used to test a single yum and apt box. 'all' can be used to\n" +
                    "    test all available boxes. The available boxes are: \n" +
                    "    ${BOXES}"
            dependsOn 'vagrantCheckVersion'
        }
    }

    private static void createPlatformTestTask(Project project) {
        project.tasks.create('platformTest') {
            group 'Verification'
            description "Test unit and integ tests on different platforms using vagrant.\n" +
                    "    Specify the vagrant boxes to test using the gradle property 'vagrant.boxes'.\n" +
                    "    'all' can be used to test all available boxes. The available boxes are: \n" +
                    "    ${BOXES}"
            dependsOn 'vagrantCheckVersion'
        }
    }

    private static void createVagrantTasks(Project project) {
        createCleanTask(project)
        createStopTask(project)
        createSmokeTestTask(project)
        createPrepareVagrantTestEnvTask(project)
        createPackagingTestTask(project)
        createPlatformTestTask(project)
    }

    private static void createVagrantBoxesTasks(Project project) {
        assert project.extensions.esvagrant.boxes != null

        assert project.extensions.esvagrant.vagrantEnvVars != null
        Map<String, String> vagrantEnvVars = project.extensions.esvagrant.vagrantEnvVars

        assert project.tasks.stop != null
        Task stop = project.tasks.stop

        assert project.tasks.vagrantSmokeTest != null
        Task vagrantSmokeTest = project.tasks.vagrantSmokeTest

        assert project.tasks.vagrantCheckVersion != null
        Task vagrantCheckVersion = project.tasks.vagrantCheckVersion

        assert project.tasks.virtualboxCheckVersion != null
        Task virtualboxCheckVersion = project.tasks.virtualboxCheckVersion

        assert project.tasks.setupPackaging != null
        Task setupPackaging = project.tasks.setupPackaging

        assert project.tasks.packagingTest != null
        Task packagingTest = project.tasks.packagingTest

        assert project.tasks.platformTest != null
        Task platformTest = project.tasks.platformTest

        // Each box gets it own set of tasks
        for (String box : BOXES) {

            String boxTask = box.capitalize().replace('-', '')
            // always add a halt task for all boxes, so clean makes sure they are all shutdown
            Task halt = project.tasks.create("vagrant${boxTask}#halt", VagrantCommandTask) {
                command 'halt'
                boxName box
                environmentVars vagrantEnvVars
                description "Runs 'vagrant halt' for box ${box}"
            }
            stop.dependsOn(halt)

            Task update = project.tasks.create("vagrant${boxTask}#update", VagrantCommandTask) {
                command 'box'
                subcommand 'update'
                boxName box
                environmentVars vagrantEnvVars
                dependsOn vagrantCheckVersion, virtualboxCheckVersion
                description "Runs 'vagrant update' for box ${box}"
            }
            update.mustRunAfter(setupPackaging)

            Task up = project.tasks.create("vagrant${boxTask}#up", VagrantCommandTask) {
                command 'up'
                boxName box
                environmentVars vagrantEnvVars
                /* Its important that we try to reprovision the box even if it already
                  exists. That way updates to the vagrant configuration take automatically.
                  That isn't to say that the updates will always be compatible. Its ok to
                  just destroy the boxes if they get busted but that is a manual step
                  because its slow-ish. */
                /* We lock the provider to virtualbox because the Vagrantfile specifies
                  lots of boxes that only work properly in virtualbox. Virtualbox is
                  vagrant's default but its possible to change that default and folks do.
                  But the boxes that we use are unlikely to work properly with other
                  virtualization providers. Thus the lock. */
                args '--provision', '--provider', 'virtualbox'
                /* It'd be possible to check if the box is already up here and output
                  SKIPPED but that would require running vagrant status which is slow! */
                dependsOn update
                description "Runs 'vagrant up' for box ${box} (with provisioning)"
            }

            // We use an Exec task here (instead of VagrantCommandTask) because we want the full output displayed
            Task smoke = project.tasks.create("vagrant${boxTask}#smoketest", Exec) {
                environment vagrantEnvVars
                dependsOn up
                finalizedBy halt
                description "Tries to run a 'hello world' shell command on box ${box}"
            }
            if (box in LINUX_BOXES) {
                smoke.commandLine = ['vagrant', 'ssh', box, '--command', """
                    set -euo pipefail
                    echo 'Hello from ${project.path}' | sed -ue 's/^/    ${box}: /'
                """]
            } else {
                smoke.commandLine = ['vagrant', 'winrm', box, '--command', wrapPowershell("""
                    Write-Host '    ${box}: Hello from ${project.path}'
                """)]
            }
            vagrantSmokeTest.dependsOn(smoke)

            Task relocateProject = project.tasks.create("vagrant${boxTask}#relocateProject", VagrantCommandTask) {
                boxName box
                environmentVars vagrantEnvVars
                dependsOn up
                finalizedBy halt
                description "Run a command on box ${box} to move the project to vagrant's home directory"
            }
            if (box in LINUX_BOXES) {
                relocateProject.command = 'ssh'
                relocateProject.args = ['--command', """
                    set -euo pipefail
                    rm -rf ~/elasticsearch ~/elasticsearch-extra
                    rsync -rlt /elasticsearch/ ~/elasticsearch
                    if [ -d /elasticsearch-extra ]; then
                        rsync -rlt /elasticsearch-extra/ ~/elasticsearch-extra
                    fi
                """]
            } else {
                relocateProject.command = 'winrm'
                relocateProject.args = ['--command', wrapPowershell("""
                    if (Test-Path "~/elasticsearch") {
                        Remove-Long-Path C:\\Users\\vagrant\\elasticsearch
                    }
                    if (Test-Path "~/elasticsearch-extra") {
                        Remove-Long-Path C:\\Users\\vagrant\\elasticsearch-extra
                    }

                    Copy-Long-Path C:\\elasticsearch C:\\Users\\vagrant\\elasticsearch

                    if (Test-Path "C:/elasticsearch-extra") {
                        Copy-Long-Path C:\\elasticsearch-extra C:\\Users\\vagrant\\elasticsearch-extra
                    }
                """)]
            }

            /*
             * Other projects can hook dependencies onto this task to get the vm ready to run the gradle build
             */
            Task prepareGradle = project.tasks.create("vagrant${boxTask}#prepareGradleBuild") {
                dependsOn relocateProject
            }
            prepareGradle.ext.box = box

            if (box in LINUX_BOXES) {
                Task batsPackagingTest = project.tasks.create("vagrant${boxTask}#batsPackagingTest", BatsOverVagrantTask) {
                    remoteCommand BATS_TEST_COMMAND
                    boxName box
                    environmentVars vagrantEnvVars
                    dependsOn up, setupPackaging
                    finalizedBy halt
                    description "Runs the BATS packaging test scripts on box ${box}"
                }

                TaskExecutionAdapter batsPackagingReproListener = new TaskExecutionAdapter() {
                    @Override
                    void afterExecute(Task task, TaskState state) {
                        if (state.failure != null) {
                            println "REPRODUCE WITH: gradle ${batsPackagingTest.path} " +
                                    "-Dtests.seed=${project.testSeed} "
                        }
                    }
                }
                batsPackagingTest.doFirst {
                    project.gradle.addListener(batsPackagingReproListener)
                }
                batsPackagingTest.doLast {
                    project.gradle.removeListener(batsPackagingReproListener)
                }
                if (project.extensions.esvagrant.boxes.contains(box)) {
                    packagingTest.dependsOn(batsPackagingTest)
                }
            }

            Task groovyPackagingTest = project.tasks.create("vagrant${boxTask}#groovyPackagingTest", VagrantCommandTask) {
                boxName box
                environmentVars vagrantEnvVars
                dependsOn up, setupPackaging, prepareGradle
                finalizedBy halt
                description "Runs the Groovy portable packaging tests on box ${box}"
            }
            if (box in LINUX_BOXES) {
                groovyPackagingTest.command = 'ssh'
                groovyPackagingTest.args = ['--command', """
                    set -euo pipefail
                    cd ~/elasticsearch
                    \$GRADLE_HOME/bin/gradle ${-> project.extensions.esvagrant.testTask} -Dtests.seed=${-> project.testSeed}
                """]
            } else {
                groovyPackagingTest.command = 'winrm'
                groovyPackagingTest.args = ['--command', wrapPowershell("""
                    cd ~/elasticsearch
                    & "\$Env:GRADLE_HOME/bin/gradle.bat" "${-> project.extensions.esvagrant.testTask}" "-Dtests.seed=${-> project.testSeed}"
                """)]
            }

            TaskExecutionAdapter groovyPackagingReproListener = new TaskExecutionAdapter() {
                @Override
                void afterExecute(Task task, TaskState state) {
                    if (state.failure != null) {
                        println "REPRODUCE WITH: gradle ${groovyPackagingTest.path} -Dtests.seed=${project.testSeed}"
                    }
                }
            }

            groovyPackagingTest.doFirst {
                project.gradle.addListener(groovyPackagingReproListener)
            }
            groovyPackagingTest.doLast {
                project.gradle.removeListener(groovyPackagingReproListener)
            }
            if (project.extensions.esvagrant.boxes.contains(box)) {
                packagingTest.dependsOn(groovyPackagingTest)
            }


            Task platform = project.tasks.create("vagrant${boxTask}#platformTest", VagrantCommandTask) {
                boxName box
                environmentVars vagrantEnvVars
                dependsOn up, prepareGradle
                finalizedBy halt
            }
            if (box in LINUX_BOXES) {
                platform.command = 'ssh'
                platform.args = ['--command', """
                    set -euo pipefail
                    cd ~/elasticsearch
                    \$GRADLE_HOME/bin/gradle test integTest -Dtests.seed=${-> project.testSeed}
                """]
            } else {
                platform.command = 'winrm'
                platform.args = ['--command', wrapPowershell("""
                    cd ~/elasticsearch
                    & "\$Env:GRADLE_HOME/bin/gradle.bat" "test" "integTest" "-Dtests.seed=${-> project.testSeed}"
                """)]
            }

            TaskExecutionAdapter platformReproListener = new TaskExecutionAdapter() {
                @Override
                void afterExecute(Task task, TaskState state) {
                    if (state.failure != null) {
                        println "REPRODUCE WITH: gradle ${platform.path} -Dtests.seed=${project.testSeed}"
                    }
                }
            }
            platform.doFirst {
                project.gradle.addListener(platformReproListener)
            }
            platform.doLast {
                project.gradle.removeListener(platformReproListener)
            }
            if (project.extensions.esvagrant.boxes.contains(box)) {
                platformTest.dependsOn(platform)
            }
        }
    }

    /*
     * The library that vagrant uses to talk to WinRM [1] executes commands in such a way that causes commands to return a success exit
     * code when $ErrorActionPreference = "Stop" is set and the script is terminated by an error. If we wrap our command inside a single
     * call to the powershell executable, then failures cause that call to return a failure exit code, which is picked up by WinRB's
     * wrapper script
     *
     * This returns GString and not String because several of the scripts that use closures that are lazily evaluated
     * (of the form "foo ${-> bar}") to pull in configuration from the plugin's extension after the plugin has already been applied
     *
     * [1] https://github.com/WinRb/WinRM/blob/52918d73590449466332aaf06f69b0cf77d91dc7/lib/winrm/shells/power_shell.rb#L99-L115
     */
    static GString wrapPowershell(GString script) {
        "powershell -Command { \$ErrorActionPreference = 'Stop'; ${-> script} }"
    }

    static GString wrapPowershell(String script) {
        wrapPowershell("${script}")
    }
}
