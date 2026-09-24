/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.runner

import spock.lang.Shared
import spock.lang.Specification

/**
 * Functional coverage for {@code .buildkite/scripts/gradle-isolated-projects-validation.sh}.
 *
 * <p>These tests run the script inside a temporary sandbox with stubbed {@code jq}
 * and {@code .ci/scripts/run-gradle.sh} commands so the failure-handling branches
 * are exercised without invoking the real CI wrapper.
 */
class GradleIsolatedProjectsValidationScriptSpec extends Specification {

    @Shared
    File repoRoot = new File(System.getProperty('root.project.dir'))

    def "fails when gradle exits before producing a problems report"() {
        given:
        def sandbox = createSandbox('''
#!/bin/bash
set -euo pipefail
exit 7
''')

        when:
        def result = runValidationScript(sandbox)

        then:
        result.exitCode == 7
        result.output.contains('Expected Gradle problems report')
        result.output.contains('Gradle command failed before a problems report was produced')
    }

    def "fails when gradle exits non-zero even if a problems report exists"() {
        given:
        def sandbox = createSandbox('''
#!/bin/bash
set -euo pipefail
mkdir -p "${WORKSPACE}/build"
cat > "${WORKSPACE}/build/problems-status.json" <<'EOF'
{
  "totalProblems" : 2,
  "severities" : [
    { "severity" : "ERROR", "count" : 2 }
  ],
  "problems" : [
    {
      "id" : "validation:configuration-cache:cannot-access-another-project",
      "displayName" : "Cannot access another project",
      "severity" : "ERROR",
      "count" : 2
    }
  ]
}
EOF
exit 9
''')

        when:
        def result = runValidationScript(sandbox)

        then:
        result.exitCode == 9
        result.output.contains('Gradle exit code: 9')
        result.output.contains('Isolated projects validation violations: 2')
        result.output.contains('Gradle command failed; isolated-projects validation requires a successful build so the violation count is trustworthy')
        !result.output.contains('Isolated projects violations are within threshold')
    }

    private RunResult runValidationScript(File sandbox, Map<String, String> env = [:]) {
        def pb = new ProcessBuilder('bash', '.buildkite/scripts/gradle-isolated-projects-validation.sh')
        pb.directory(sandbox)
        pb.redirectErrorStream(true)
        pb.environment().put('WORKSPACE', new File(sandbox, 'workspace').absolutePath)
        pb.environment().put('PATH', new File(sandbox, 'bin').absolutePath + File.pathSeparator + System.getenv('PATH'))
        pb.environment().putAll(env)

        def process = pb.start()
        def output = new StringBuilder()
        process.inputStream.eachLine { line ->
            output.append(line).append('\n')
        }
        process.waitFor()
        return new RunResult(exitCode: process.exitValue(), output: output.toString())
    }

    private File createSandbox(String runGradleScript) {
        File sandbox = java.nio.file.Files.createTempDirectory('isolated-projects-validation-script').toFile()
        File workspace = new File(sandbox, 'workspace')
        workspace.mkdirs()

        File scriptDir = new File(sandbox, '.buildkite/scripts')
        scriptDir.mkdirs()
        File ciDir = new File(sandbox, '.ci/scripts')
        ciDir.mkdirs()
        File binDir = new File(sandbox, 'bin')
        binDir.mkdirs()

        File sourceScript = new File(repoRoot, '.buildkite/scripts/gradle-isolated-projects-validation.sh')
        File targetScript = new File(scriptDir, 'gradle-isolated-projects-validation.sh')
        targetScript.text = sourceScript.text
        targetScript.setExecutable(true)

        File runGradle = new File(ciDir, 'run-gradle.sh')
        runGradle.text = runGradleScript.stripIndent().trim() + '\n'
        runGradle.setExecutable(true)

        File jq = new File(binDir, 'jq')
        jq.text = '''
#!/bin/bash
set -euo pipefail
query="$2"
json_file="$3"
python3 - "$query" "$json_file" <<'PY'
import json
import sys
from collections import defaultdict

query = sys.argv[1]
json_file = sys.argv[2]
with open(json_file, 'r', encoding='utf-8') as f:
    report = json.load(f)

problems = [p for p in report.get('problems', []) if p.get('id', '').startswith('validation:configuration-cache:')]
if 'add // 0' in query:
    print(sum(int(p.get('count', 0)) for p in problems))
else:
    print('Severity breakdown (isolated-projects validation only):')
    severity_counts = defaultdict(int)
    for problem in problems:
        severity_counts[problem.get('severity', 'UNKNOWN')] += int(problem.get('count', 0))
    for severity in sorted(severity_counts):
        print(f'- {severity}: {severity_counts[severity]}')
    print()
    print('Top 10 isolated-projects problem IDs:')
    for problem in problems[:10]:
        print(f"- {problem.get('count', 0)}x {problem.get('id')} ({problem.get('severity')})")
PY
'''.stripIndent().trim() + '\n'
        jq.setExecutable(true)

        return sandbox
    }

    static class RunResult {
        int exitCode
        String output
    }
}
