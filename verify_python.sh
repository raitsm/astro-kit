#!/bin/bash

#
# function to validate presence of Python packages and the version number of these packages
# invoke as: check_python_package "package_name" "version"
# note that the function does not require to specify a complete version. 
# For example, version "2.2" will give a match also for "2.2.4" and so on.
#
function check_python_package() {
  local expected_pkg_name=$1
  local expected_pkg_version=$2
  echo ">>> Checking if Python package ${expected_pkg_name} v${expected_pkg_version} is installed"
  pkg_actual=$(pip3 freeze | grep "$expected_pkg_name==")
  if [ -z "$pkg_actual" ]; then
    echo "FAIL: Python package ${expected_pkg_name} not found, need to install it. Please note it should be version ${expected_pkg_version}"
    exit 1
  else
    echo "OK: Python package ${expected_pkg_name} found on the system, proceeding with version check"
  fi

  pkg_version_actual=$(pip3 freeze | grep "^$expected_pkg_name==$expected_pkg_version")
  if [ -z "$pkg_version_actual" ]; then
    echo "FAIL: Python package ${expected_pkg_name} has a wrong version. The project requires ${expected_pkg_version}"
    exit 1 
  else
    echo "OK: Python package ${expected_pkg_name} has version ${expected_pkg_version} as expected."
  fi
}


python_expected="Python 3.8"
echo "Script to validate existence and versions for Python and the libraries required"
echo "------------------------------------------------"

echo ">>> Checking if Python3 is installed."
python3 -V  > /dev/null 2>&1
if [ $? -eq 0 ]; then
  echo "OK: Python3 installed"
else
  echo "FAIL: Python3 is not installed."
  exit 1
fi

echo ">>> Checking if Python version is ${python_expected}."
python_actual=$(python3 -V 2>&1)
if [[ "$python_actual" == "$python_expected"* ]]; then
  echo "OK: ${python_actual} installed"
else
  echo "FAIL: ${python_actual} found, need to install ${python_expected}"
  exit 1
fi

#
# check the presence and versions of Python packages
# call function check_python_package "package_name" "package_version"
# note that the validation checks only for most significant digits in the version number, for example:
# "2.22" will give a match for "2.22" as well as "2.22.1" and so on.
#
check_python_package "requests" "2.22"
check_python_package "mysql-connector" "2.2.9"
