#!/usr/bin/env ruby
# Licensed to  ElasticSearch and Shay Banon under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the 'License'); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License
#
# generate property file for the jdk randomization test
# 
#
require 'yaml'

class JDKSelector
  attr_reader :directory, :jdk_list

  def initialize(directory)
    @directory = directory
  end

  # get selection of available jdks from jenkins automatic install directory
  def get_jdk
    @jdk_list = Dir.entries(directory).select do |x| 
      x.chars.first == 'J' 
    end.map do |y|
      File.join(directory, y)
    end
    self
  end

  # do ranomize selection from a given array
  def select_one(selection_array = nil)
    selection_array ||= @jdk_list
    selection_array[rand(selection_array.size)]
  end
end

# given a jdk directory selection, generate revelant environment variables
def get_env_matrix(data_array)
  [*data_array].map do |x|
    {
      'PATH' => File.join(x,'bin') + ':' + ENV['PATH'],
      'JAVA_HOME' => x,
      'BUILD_DESC' => File.basename(x)
    }
  end
end

# pick first element out of array of hashes, generate write java property file
def generate_property_file(directory, data)
  #array transformation
  content = data.first.map do |key, value|
    "%s=%s"%[key, value]
  end
  file_name = (ENV['BUILD_ID'] + ENV['BUILD_NUMBER']) || 'prop'
  file_name = file_name.split(File::SEPARATOR).first + '.txt'
  File.open(File.join(directory, file_name), 'w') do |file| 
              file.write(content.join("\n")) 
  end
end

# jenkins sets pwd prior to execution
jdk_selector = JDKSelector.new(File.join(ENV['PWD'],'tools','hudson.model.JDK'))
environment_matrix = get_env_matrix(jdk_selector.get_jdk.select_one)

working_directory = ENV['WORKSPACE'] || '/var/tmp'
generate_property_file(working_directory, environment_matrix)
