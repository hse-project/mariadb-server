# Copyright (c) 2023 Micron Technology, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1335 USA

find_package(PkgConfig)

MACRO(SKIP_HSE_PLUGIN msg)
  MESSAGE_ONCE(SKIP_HSE_PLUGIN "Can't build the HSE plugin - ${msg}")
  ADD_FEATURE_INFO(HSE "OFF" "Storage Engine")
  RETURN()
ENDMACRO()

IF (NOT PKG_CONFIG_FOUND)
  SKIP_HSE_PLUGIN("pkg-config not found.")
ENDIF()

IF (NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  SKIP_HSE_PLUGIN("HSE only supports Linux.")
ENDIF()

IF (NOT CMAKE_SYSTEM_PROCESSOR MATCHES "(x86_64|s390x)")
  SKIP_HSE_PLUGIN("HSE only supports x86_64 and s390x.")
ENDIF()

pkg_check_modules(HSE hse-3>=3.0.0)

IF (NOT HSE_FOUND)
  SKIP_HSE_PLUGIN("libhse-3 not found.")
ENDIF()

SET(HSE_SOURCES ha_hse.cc ha_hse.h)
MYSQL_ADD_PLUGIN(hse ${HSE_SOURCES} STORAGE_ENGINE)
target_include_directories(hse PRIVATE ${HSE_INCLUDE_DIRS})
target_compile_options(hse PRIVATE ${HSE_CFLAGS_OTHER})
target_link_libraries(hse ${HSE_LIBRARIES})
