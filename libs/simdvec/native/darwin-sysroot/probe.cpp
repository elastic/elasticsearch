/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/*
 * This simple cpp program defines the "contract" for the assembled Darwin sysroot.
 * The Darwin sysroot will include all of libc, libm, libpthread and libmalloc, but we
 * want to avoid bringing in all the xnu kernel headers too; therefore, it will be
 * the union of the system headers that the native libraries built with it actually
 * include (simdvec and simdjson today).
 *
 * assemble.sh compiles this program twice, both times with -fsyntax-only. First against the
 * full xnu staging tree, using the compiler's dependency output to compute the exact set of
 * xnu headers reachable from here. That computed closure is what lands in the sysroot. Then
 * again on the reduced set, to prove the sysroot is self-contained.
 *
 * This file defines what the sysroot supports. A library that needs a system header
 * not reachable from here will fail to compile until the include is added and the
 * toolchain image is rebuilt.
 *
 * C++20/23 headers that simdjson only reaches behind __cplusplus guards (<ranges>,
 * <concepts>, <expected>, <meta>) are omitted; the native builds use -std=c++17.
 */

/* C headers */
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <strings.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <machine/endian.h>

/* Target/builtin headers, supplied by the clang resource directory rather than the sysroot */
#include <arm_neon.h>
#include <arm_sve.h>

/* C++ headers used by libvec */
#include <algorithm>
#include <limits>
#include <type_traits>
#include <utility>

/* C++ headers used by libsimdjson */
#include <array>
#include <atomic>
#include <cassert>
#include <cfloat>
#include <charconv>
#include <climits>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <cwchar>
#include <functional>
#include <initializer_list>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include <version>

/* This file is only ever preprocessed and parsed: the include list above is all of it. */
