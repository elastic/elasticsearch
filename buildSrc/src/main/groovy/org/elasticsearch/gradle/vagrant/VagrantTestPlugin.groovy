package org.elasticsearch.gradle.vagrant

import com.carrotsearch.gradle.junit4.RandomizedTestingPlugin
import org.apache.tools.ant.taskdefs.condition.Os
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

    /** All available boxes **/
    static List<String> BOXES = [
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

    /** Boxes used when sampling the tests **/
    static List<String> SAMPLE = [
            'centos-7',
            'ubuntu-1404',
    ]

    /** All onboarded archives by default, available for Bats tests even if not used **/
    static List<String> DISTRIBUTION_ARCHIVES = ['tar', 'rpm', 'deb']

    /** Packages onboarded for upgrade tests **/
    static List<String> UPGRADE_FROM_ARCHIVES = ['rpm', 'deb']

    private static final BATS = 'bats'
    private static final String BATS_TEST_COMMAND ="cd \$BATS_ARCHIVES && sudo bats --tap \$BATS_TESTS/*.$BATS"
    private static final String PLATFORM_TEST_COMMAND ="rm -rf ~/elasticsearch && rsync -r /elasticsearch/ ~/elasticsearch && cd ~/elasticsearch && ./gradlew test integTest"

    @Override
    void apply(Project project) {

        // Creates the Vagrant extension for the project
        project.extensions.create('esvagrant', VagrantPropertiesExtension, listVagrantBoxes(project))

        // Add required repositories for Bats tests
        configureBatsRepositories(project)

        // Creates custom configurations for Bats testing files (and associated scripts and archives)
        createBatsConfiguration(project)

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

    private List<String> listVagrantBoxes(Project project) {
        String vagrantBoxes = project.getProperties().get('vagrant.boxes', 'sample')
        if (vagrantBoxes == 'sample') {
            return SAMPLE
        } else if (vagrantBoxes == 'all') {
            return BOXES
        } else {
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

    private static void createBatsConfiguration(Project project) {
        project.configurations.create(BATS)

        String upgradeFromVersion = System.getProperty("tests.packaging.upgradeVersion");
        if (upgradeFromVersion == null) {
            String firstPartOfSeed = project.rootProject.testSeed.tokenize(':').get(0)
            final long seed = Long.parseUnsignedLong(firstPartOfSeed, 16)
            final def indexCompatVersions = project.versionCollection.versionsIndexCompatibleWithCurrent
            upgradeFromVersion = indexCompatVersions[new Random(seed).nextInt(indexCompatVersions.size())]
        }

        DISTRIBUTION_ARCHIVES.each {
            // Adds a dependency for the current version
            project.dependencies.add(BATS, project.dependencies.project(path: ":distribution:${it}", configuration: 'archives'))
        }

        UPGRADE_FROM_ARCHIVES.each {
            // The version of elasticsearch that we upgrade *from*
            project.dependencies.add(BATS, "org.elasticsearch.distribution.${it}:elasticsearch:${upgradeFromVersion}@${it}")
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
        File batsDir = new File("${project.buildDir}/${BATS}")

        Task createBatsDirsTask = project.tasks.create('createBatsDirs')
        createBatsDirsTask.outputs.dir batsDir
        createBatsDirsTask.doLast {
            batsDir.mkdirs()
        }

        Copy copyBatsArchives = project.tasks.create('copyBatsArchives', Copy) {
            dependsOn createBatsDirsTask
            into "${batsDir}/archives"
            from project.configurations[BATS]
        }

        Copy copyBatsTests = project.tasks.create('copyBatsTests', Copy) {
            dependsOn createBatsDirsTask
            into "${batsDir}/tests"
            from {
                "${project.extensions.esvagrant.batsDir}/tests"
            }
        }

        Copy copyBatsUtils = project.tasks.create('copyBatsUtils', Copy) {
            dependsOn createBatsDirsTask
            into "${batsDir}/utils"
            from {
                "${project.extensions.esvagrant.batsDir}/utils"
            }
        }

        // Now we iterate over dependencies of the bats configuration. When a project dependency is found,
        // we bring back its own archives, test files or test utils.
        project.afterEvaluate {
            project.configurations.bats.dependencies.findAll {it.targetConfiguration == BATS }.each { d ->
                if (d instanceof DefaultProjectDependency) {
                    DefaultProjectDependency externalBatsDependency = (DefaultProjectDependency) d
                    Project externalBatsProject = externalBatsDependency.dependencyProject
                    String externalBatsDir = externalBatsProject.extensions.esvagrant.batsDir

                    if (project.extensions.esvagrant.inheritTests) {
                        copyBatsTests.from(externalBatsProject.files("${externalBatsDir}/tests"))
                    }
                    if (project.extensions.esvagrant.inheritTestArchives) {
                        copyBatsArchives.from(externalBatsDependency.projectConfiguration.files)
                    }
                    if (project.extensions.esvagrant.inheritTestUtils) {
                        copyBatsUtils.from(externalBatsProject.files("${externalBatsDir}/utils"))
                    }
                }
            }
        }

        Task createVersionFile = project.tasks.create('createVersionFile', FileContentsTask) {
            dependsOn createBatsDirsTask
            file "${batsDir}/archives/version"
            contents project.version
        }

        Task createUpgradeFromFile = project.tasks.create('createUpgradeFromFile', FileContentsTask) {
            dependsOn createBatsDirsTask
            file "${batsDir}/archives/upgrade_from_version"
            contents project.extensions.esvagrant.upgradeFromVersion
        }

        Task vagrantSetUpTask = project.tasks.create('setupBats')
        vagrantSetUpTask.dependsOn 'vagrantCheckVersion'
        vagrantSetUpTask.dependsOn copyBatsTests, copyBatsUtils, copyBatsArchives, createVersionFile, createUpgradeFromFile
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

        assert project.tasks.stop != null
        Task stop = project.tasks.stop

        assert project.tasks.vagrantSmokeTest != null
        Task vagrantSmokeTest = project.tasks.vagrantSmokeTest

        assert project.tasks.vagrantCheckVersion != null
        Task vagrantCheckVersion = project.tasks.vagrantCheckVersion

        assert project.tasks.virtualboxCheckVersion != null
        Task virtualboxCheckVersion = project.tasks.virtualboxCheckVersion

        assert project.tasks.setupBats != null
        Task setupBats = project.tasks.setupBats

        assert project.tasks.packagingTest != null
        Task packagingTest = project.tasks.packagingTest

        assert project.tasks.platformTest != null
        Task platformTest = project.tasks.platformTest

        /*
         * We always use the main project.rootDir as Vagrant's current working directory (VAGRANT_CWD)
         * so that boxes are not duplicated for every Gradle project that use this VagrantTestPlugin.
         */
        def vagrantEnvVars = [
                'VAGRANT_CWD'           : "${project.rootDir.absolutePath}",
                'VAGRANT_VAGRANTFILE'   : 'Vagrantfile',
                'VAGRANT_PROJECT_DIR'   : "${project.projectDir.absolutePath}"
        ]

        // Each box gets it own set of tasks
        for (String box : BOXES) {
            String boxTask = box.capitalize().replace('-', '')

            // always add a halt task for all boxes, so clean makes sure they are all shutdown
            Task halt = project.tasks.create("vagrant${boxTask}#halt", VagrantCommandTask) {
                command 'halt'
                boxName box
                environmentVars vagrantEnvVars
            }
            stop.dependsOn(halt)

            Task update = project.tasks.create("vagrant${boxTask}#update", VagrantCommandTask) {
                command 'box'
                subcommand 'update'
                boxName box
                environmentVars vagrantEnvVars
                dependsOn vagrantCheckVersion, virtualboxCheckVersion
            }
            update.mustRunAfter(setupBats)

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
            }

            Task smoke = project.tasks.create("vagrant${boxTask}#smoketest", Exec) {
                environment vagrantEnvVars
                dependsOn up
                finalizedBy halt
                commandLine 'vagrant', 'ssh', box, '--command',
                        "set -o pipefail && echo 'Hello from ${project.path}' | sed -ue 's/^/    ${box}: /'"
            }
            vagrantSmokeTest.dependsOn(smoke)

            Task packaging = project.tasks.create("vagrant${boxTask}#packagingTest", BatsOverVagrantTask) {
                remoteCommand BATS_TEST_COMMAND
                boxName box
                environmentVars vagrantEnvVars
                dependsOn up, setupBats
                finalizedBy halt
            }

            TaskExecutionAdapter packagingReproListener = new TaskExecutionAdapter() {
                @Override
                void afterExecute(Task task, TaskState state) {
                    final String gradlew = Os.isFamily(Os.FAMILY_WINDOWS) ? "gradlew" : "./gradlew"
                    if (state.failure != null) {
                        println "REPRODUCE WITH: ${gradlew} ${packaging.path} " +
                            "-Dtests.seed=${project.testSeed} "
                    }
                }
            }
            packaging.doFirst {
                project.gradle.addListener(packagingReproListener)
            }
            packaging.doLast {
                project.gradle.removeListener(packagingReproListener)
            }
            if (project.extensions.esvagrant.boxes.contains(box)) {
                packagingTest.dependsOn(packaging)
            }

            Task platform = project.tasks.create("vagrant${boxTask}#platformTest", VagrantCommandTask) {
                command 'ssh'
                boxName box
                environmentVars vagrantEnvVars
                dependsOn up
                finalizedBy halt
                args '--command', PLATFORM_TEST_COMMAND + " -Dtests.seed=${-> project.testSeed}"
            }
            TaskExecutionAdapter platformReproListener = new TaskExecutionAdapter() {
                @Override
                void afterExecute(Task task, TaskState state) {
                    if (state.failure != null) {
                        println "REPRODUCE WITH: gradle ${platform.path} " +
                            "-Dtests.seed=${project.testSeed} "
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
}
