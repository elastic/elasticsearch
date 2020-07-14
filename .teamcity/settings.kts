import jetbrains.buildServer.configs.kotlin.v2019_2.DslContext
import jetbrains.buildServer.configs.kotlin.v2019_2.project
import jetbrains.buildServer.configs.kotlin.v2019_2.vcs.GitVcsRoot
import jetbrains.buildServer.configs.kotlin.v2019_2.version

version = "2020.1"

val developmentBranches = listOf("master", "7.x", "7.8", "6.8")

project {
    subProjectsOrder = developmentBranches.map { devBranch ->
        subProject {
            id(devBranch.replace('.', '_'))
            name = devBranch

            vcsRoot(createVcsRoot(devBranch))
        }
    }.map { it.id!! }
}

fun createVcsRoot(branchName: String): GitVcsRoot {
    return GitVcsRoot {
        id("${DslContext.projectName}_${branchName.replace('.', '_')}")

        name = "${DslContext.projectName} ($branchName)"
        url = "https://github.com/elastic/${DslContext.projectName.toLowerCase()}.git"
        branch = "refs/heads/$branchName"
    }
}
