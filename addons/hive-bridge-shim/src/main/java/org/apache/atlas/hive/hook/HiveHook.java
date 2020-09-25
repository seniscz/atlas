/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.hive.hook;


import org.apache.atlas.plugin.classloader.AtlasPluginClassLoader;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hive hook used for atlas entity registration.
 * <p>
 * ExecuteWithHookContext 在hive 的执行计划前后调用
 * hive-site.xml 中的配置为：
 * 1、hive.exec.pre.hooks     在执行引擎执行查询之前被调用
 * 2、hive.exec.post.hooks    在执行计划执行结束结果返回给用户之前被调用
 * 3、hive.exec.failure.hooks 在执行计划失败之后被调用
 */
public class HiveHook implements ExecuteWithHookContext {
    private static final Logger LOG = LoggerFactory.getLogger(HiveHook.class);

    private static final String ATLAS_PLUGIN_TYPE = "hive";
    private static final String ATLAS_HIVE_HOOK_IMPL_CLASSNAME = "org.apache.atlas.hive.hook.HiveHook";

    private AtlasPluginClassLoader atlasPluginClassLoader = null;
    private ExecuteWithHookContext hiveHookImpl = null;

    public HiveHook() {
        this.initialize();
    }

    @Override
    public void run(final HookContext hookContext) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HiveHook.run({})", hookContext);
        }

        try {
            activatePluginClassLoader();
            hiveHookImpl.run(hookContext);
        } finally {
            deactivatePluginClassLoader();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HiveHook.run({})", hookContext);
        }
    }

    /**
     * 初始化
     */
    private void initialize() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HiveHook.initialize()");
        }
        try {
            // 获得该类的对象实例
            atlasPluginClassLoader = AtlasPluginClassLoader.getInstance(ATLAS_PLUGIN_TYPE, this.getClass());

            /*加载 HiveHook 类
             *  Class.forName(String name, boolean initialize, ClassLoader loader)
             *  name 表示的是类的全名
             *  initialize 表示是否初始化类
             *  loader 表示加载时使用的类加载器
             */
            @SuppressWarnings("unchecked")
            Class<ExecuteWithHookContext> cls = (Class<ExecuteWithHookContext>) Class
                    .forName(ATLAS_HIVE_HOOK_IMPL_CLASSNAME, true, atlasPluginClassLoader);

            activatePluginClassLoader();

            //获得 ExecuteWithHookContext 类的对象实例
            hiveHookImpl = cls.newInstance();
        } catch (Exception excp) {
            LOG.error("Error instantiating Atlas hook implementation", excp);
        } finally {
            deactivatePluginClassLoader();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HiveHook.initialize()");
        }
    }

    private void activatePluginClassLoader() {
        if (atlasPluginClassLoader != null) {
            atlasPluginClassLoader.activate();
        }
    }

    private void deactivatePluginClassLoader() {
        if (atlasPluginClassLoader != null) {
            atlasPluginClassLoader.deactivate();
        }
    }
}
