import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import ckanext.cloudstorage.logic.action.create as create

class WroPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IResourceController, inherit=True) # temp testing 
    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic',
            'wro')
        toolkit.add_resource('assets','template_change_assets')

    
    # IActions:
    
    def get_actions(self):
        """
            overriding the default creation to use
            google cloud url mapping for resources
        """
        return{
            'resource_create':create.resource_create
        }

    
    # IResourceController

    def before_show(self,resource_dict):
        """
            overriding the display to show google cloud
            GCS url, instead of the default local mapping 
        """
        if resource_dict['url_type'] == 'upload':
            if resource_dict['name'] != '':
                res_name = resource_dict['name'].lower()
                res_format = resource_dict['format'].lower()
                resource_dict['url'] = f'https://storage.cloud.google.com/mohabtester/{res_name}.{res_format}'