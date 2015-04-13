#!/bin/env ruby
# Licensed to Elasticsearch under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance  with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on 
# an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific
# language governing permissions and limitations under the License
#
# The script extract license information out of logstash project
# First it will use license information out of gem object.  
# If that is not available, it will try to print out the license file
#
require 'rubygems' 
require 'bundler/setup'
require 'yaml'

# get all local installed gems
def all_installed_gems
   Gem::Specification.all = nil    
   all = Gem::Specification 
   Gem::Specification.reset
   all
end

all_installed_gems.select {|y| y.gem_dir.include?('vendor') }.sort {|v, u| v.name  <=> u.name }.each do |x|
  puts '='*80 #separator
  if(x.license) #ah gem has license information
    puts "%s,%s,%s,%s,%s"%[x.name, x.version, x.license, x.homepage, x.email] 
  else
    puts "%s,%s,%s,%s"%[x.name, x.version, x.homepage, x.email]
    license_file =  Dir.glob(File.join(x.gem_dir,'LICENSE*')).first #see if there is a license file
    if(license_file)
      file = File.open(license_file, 'r')
      data = file.read
      file.close
      puts data
    else
      #no license file.. try to print some gem information
      puts 'No License File'
      puts x.gem_dir
      puts x.files.join("\n")
    end
  end
end

