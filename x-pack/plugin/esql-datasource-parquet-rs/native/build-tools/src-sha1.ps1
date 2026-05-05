# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

# Windows companion to src-sha1.sh. Must produce the same output for any given
# source tree, since the hash is used as the version of the native artifact
# published to Artifactory.

$ErrorActionPreference = "Stop"

$nativeDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

# Collect paths under .cargo and src, plus Cargo.toml and Cargo.lock.
# Use forward slashes so the sort key matches `find | sort` from src-sha1.sh.
$paths = New-Object System.Collections.Generic.List[string]
foreach ($subdir in @(".cargo", "src")) {
    $base = Join-Path $nativeDir $subdir
    if (Test-Path $base) {
        Get-ChildItem -Path $base -Recurse -File | ForEach-Object {
            $rel = [System.IO.Path]::GetRelativePath($nativeDir, $_.FullName)
            $paths.Add($rel.Replace('\', '/'))
        }
    }
}
$paths.Add("Cargo.toml")
$paths.Add("Cargo.lock")

# Byte-order sort (matches `sort` under the C locale used in CI).
$sorted = $paths.ToArray()
[Array]::Sort($sorted, [System.StringComparer]::Ordinal)

$sha1 = [System.Security.Cryptography.SHA1]::Create()
try {
    foreach ($rel in $sorted) {
        $abs = Join-Path $nativeDir ($rel.Replace('/', [System.IO.Path]::DirectorySeparatorChar))
        $bytes = [System.IO.File]::ReadAllBytes($abs)
        # Strip CR (0x0D) to match `tr -d '\r'` in src-sha1.sh.
        $out = New-Object byte[] $bytes.Length
        $j = 0
        for ($i = 0; $i -lt $bytes.Length; $i++) {
            if ($bytes[$i] -ne 0x0D) {
                $out[$j] = $bytes[$i]
                $j++
            }
        }
        if ($j -gt 0) {
            [void]$sha1.TransformBlock($out, 0, $j, $null, 0)
        }
    }
    [void]$sha1.TransformFinalBlock([byte[]]::new(0), 0, 0)
    $hash = $sha1.Hash
} finally {
    $sha1.Dispose()
}

-join ($hash | ForEach-Object { $_.ToString('x2') })
