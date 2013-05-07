/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.locator;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ec2LikeMultiRegionSnitch extends Ec2MultiRegionSnitch
{

	protected static final Logger logger = LoggerFactory.getLogger(Ec2LikeMultiRegionSnitch.class);

	private static final String regex;
	private static final Pattern pattern;

	static {
		regex = DatabaseDescriptor.ec2likeAvailabilityZoneRegex();
		pattern = Pattern.compile(regex);
	}

	public Ec2LikeMultiRegionSnitch() throws IOException, ConfigurationException
	{
		super();
	}

	@Override
	protected String computeEc2Zone(String availabilityZone) {
		Matcher m = Ec2LikeMultiRegionSnitch.pattern.matcher(availabilityZone);
		if (m.matches()) {
			String rackName = m.group(3);
			if (rackName == null || "".equals(rackName)) {
				logger.info("Will use default rack name default-rack.");
				return "default-rack";
			} else {
				logger.info("Will use rack name " + rackName + ".");
				return rackName;
			}
		} else {
			logger.warn("Couldn't parse " + availabilityZone
					+ ", will use the whole name as rack.");
			return availabilityZone;
		}
	}

	@Override
	protected String computeEc2Region(String availabilityZone)
	{
		Matcher m = Ec2LikeMultiRegionSnitch.pattern.matcher(availabilityZone);
		if (m.matches()) {
			String dcName = m.group(1) + "-" + m.group(2);
			logger.info("Will use " + dcName + " as datacenter name.");
			return dcName;
		} else {
			logger.warn("Couldn't parse " + availabilityZone
					+ ", will use the whole name as datacenter.");
			return availabilityZone;
		}
	}
}
