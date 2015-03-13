'''
Working examples - simple tag report.
'''

from ckan import model
from ckan.lib.helpers import OrderedDict
from ckanext.report import lib
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import between
from sqlalchemy import cast, Numeric
import collections


def tagless_report(organization, include_sub_organizations=False):
    '''
    Produces a report on packages without tags.
    Returns something like this:
        {
         'table': [
            {'name': 'river-levels', 'title': 'River levels', 'notes': 'Harvested', 'user': 'bob', 'created': '2008-06-13T10:24:59.435631'},
            {'name': 'co2-monthly', 'title' 'CO2 monthly', 'notes': '', 'user': 'bob', 'created': '2009-12-14T08:42:45.473827'},
            ],
         'num_packages': 56,
         'packages_without_tags_percent': 4,
         'average_tags_per_package': 3.5,
        }
    '''
    # Find the packages without tags
    q = model.Session.query(model.Package) \
             .outerjoin(model.PackageTag) \
             .filter(model.PackageTag.id == None)
    '''if organization:
        q = lib.filter_by_organizations(q, organization,
                                        include_sub_organizations)'''
    tagless_pkgs = [OrderedDict((
            ('name', pkg.name),
            ('title', pkg.title),
            ('notes', lib.dataset_notes(pkg)),
            ('created', pkg.metadata_created.isoformat()),
            )) for pkg in q.slice(0,5)]

    # Average number of tags per package
    q = model.Session.query(model.Package)
    q = lib.filter_by_organizations(q, organization, include_sub_organizations)
    num_packages = q.count()
    q = q.join(model.PackageTag)
    num_taggings = q.count()
    if num_packages:
        average_tags_per_package = round(float(num_taggings) / num_packages, 1)
    else:
        average_tags_per_package = None
    packages_without_tags_percent = lib.percent(len(tagless_pkgs), num_packages)

    return {
        'table': tagless_pkgs,
        'num_packages': num_packages,
        'packages_without_tags_percent': packages_without_tags_percent,
        'num_packages': num_packages,
        'packages_without_tags_percent': packages_without_tags_percent,
        'average_tags_per_package': average_tags_per_package,
        }


def broken_link_report(organization, include_sub_organizations=False):
    
    if not organization:
        grp_totals = {}

        sql = model.Session.query(model.Group.name, func.count(model.Package.id.distinct()).label('total_grp_pkg_count'), \
               func.count(model.Resource.id.distinct()).label('total_grp_res_cnt')) \
               .join(model.Package, model.Group.id == model.Package.owner_org) \
               .join(model.ResourceGroup, model.ResourceGroup.package_id == model.Package.id) \
               .join(model.Resource, model.Resource.resource_group_id == model.ResourceGroup.id) \
               .filter(model.Group.is_organization == True) \
               .filter(model.Package.state == 'active') \
               .filter(model.Resource.state == 'active') \
               .filter(model.ResourceGroup.state == 'active') \
               .filter(model.Group.state == 'active') \
               .group_by(model.Group.name)

        for row in sql:
            grp_totals[row.name] =  OrderedDict((
                                            ('total_grp_pkg_count', row.total_grp_pkg_count),
                                            ('total_grp_res_cnt', row.total_grp_res_cnt),
                                       ))

        broken_links = []
        order = {'organization_name': 1, 'organization_title' : 2, 'broken_package_count' : 3, 'broken_resource_count' : 4, 'broken_resource_percent' : 5, 'package_count' : 6, 'broken_package_percent' : 7}
        
        q = model.Session.query(model.Group.name.label('grp_name'), model.Group.title.label('grp_title'),  \
                  func.count(model.Package.name.distinct()).label('dataset_cnt'), \
                  func.count(model.Group.name).label('broken_link_cnt')) \
                 .join(model.Package, model.Group.id == model.Package.owner_org) \
                 .join(model.ResourceGroup, model.ResourceGroup.package_id == model.Package.id) \
                 .join(model.Resource, model.Resource.resource_group_id == model.ResourceGroup.id) \
                 .join(model.TaskStatus, model.TaskStatus.entity_id == model.Resource.id) \
                 .filter(model.Group.is_organization == True) \
                 .filter(between(cast(model.TaskStatus.value, Numeric(3)), 400, 600)) \
                 .filter(cast(model.TaskStatus.value, Numeric(3)) != 511) \
                 .filter(model.TaskStatus.key == 'error_code' )\
                 .filter(model.Package.state == 'active') \
                 .filter(model.Resource.state == 'active') \
                 .filter(model.ResourceGroup.state == 'active') \
                 .filter(model.Group.state == 'active') \
                 .group_by(model.Group.name, model.Group.title) \
                 .order_by(model.Group.title)

        for row in q:

          broken_links.append(OrderedDict((
                   ('organization_name', row.grp_name),
                   ('organization_title', row.grp_title),
                   ('broken_package_count', row.dataset_cnt),
                   ('broken_resource_count', row.broken_link_cnt),
                   ('broken_resource_percent', lib.percent(row.broken_link_cnt, grp_totals[row.grp_name]['total_grp_res_cnt'])),
                   ('package_count', grp_totals[row.grp_name]['total_grp_pkg_count']),
                   ('broken_package_percent', lib.percent(row.dataset_cnt, grp_totals[row.grp_name]['total_grp_pkg_count'])),
                )) )
               
        #if organization:
        #    q = lib.filter_by_organizations(q, organization,
        #                                    include_sub_organizations)        
        
        q = q.subquery()
        q1 = model.Session.query(func.sum(q.c.dataset_cnt).label('total_brkn_dataset'), func.sum(q.c.broken_link_cnt).label('total_brkn_link')).first()

        total_brkn_dataset = q1.total_brkn_dataset	     
        total_brkn_link = q1.total_brkn_link
        
    else:
        broken_links = []
        order = {'dataset_name':1, 'dataset_title':2, 'resource_id':3, 'resource_position':4, 'resource_url':5, 'reason':6, 'failure_count':7}

        
        sql = model.Session.query(model.Package.name, model.Package.title, model.Resource.id, model.Resource.position, model.Resource.url, model.TaskStatus.value, model.TaskStatus.entity_id) \
                     .join(model.Group, model.Group.id == model.Package.owner_org) \
                     .join(model.ResourceGroup, model.ResourceGroup.package_id == model.Package.id) \
                     .join(model.Resource, model.Resource.resource_group_id == model.ResourceGroup.id) \
                     .join(model.TaskStatus, model.TaskStatus.entity_id == model.Resource.id) \
                     .filter(model.Group.is_organization == True) \
                     .filter(between(cast(model.TaskStatus.value, Numeric(3)), 400, 600)) \
                     .filter(cast(model.TaskStatus.value, Numeric(3)) != 511) \
                     .filter(model.TaskStatus.key == 'error_code' )\
                     .filter(model.Package.state == 'active') \
                     .filter(model.Resource.state == 'active') \
                     .filter(model.ResourceGroup.state == 'active') \
                     .filter(model.Group.state == 'active') \
        			 .filter(model.Group.name == organization)
 
        for row in sql:
           q = model.Session.query(model.TaskStatus.value) \
                   .filter(model.TaskStatus.key == 'openness_score_failure_count') \
                   .filter(model.TaskStatus.entity_id == row.entity_id)
        
           q2 = model.Session.query(model.TaskStatus.value) \
                   .filter(model.TaskStatus.key == 'openness_score_reason') \
                   .filter(model.TaskStatus.entity_id == row.entity_id) 
                                    
           broken_links.append(OrderedDict((
                      ('dataset_name', row.name),
                      ('dataset_title', row.title),
                      ('resource_id', row.id),
                      ('resource_position', row.position),
                      ('resource_url', row.url),
                      ('reason', q2.first().value),
                      ('failure_count', q.first().value),
                   )) )
                 
                   
        sql = sql.subquery()
        
        q1 = model.Session.query(func.count(sql.c.name.distinct()).label('total_brkn_dataset'), func.count(sql.c.id).label('total_brkn_link')).first()
       
        total_brkn_dataset = q1.total_brkn_dataset	     
        total_brkn_link = q1.total_brkn_link
        
    # Number of total resources and package
    q = model.Session.query(model.Package)\
                           .filter(model.Package.state == 'active')

    if organization:
        q = lib.filter_by_organizations(q, organization, include_sub_organizations)

    num_packages = q.count()

    q = q.join(model.Group, model.Group.id == model.Package.owner_org)\
         .join(model.ResourceGroup) \
         .join(model.Resource) \
         .filter(model.Package.state == 'active') \
         .filter(model.Resource.state == 'active') \
         .filter(model.ResourceGroup.state == 'active') \
         .filter(model.Group.state == 'active')

    num_res = q.count()

    resultData = []
    resultData.append( OrderedDict((
               ('num_broken_packages', total_brkn_dataset),
               ('num_packages', num_packages),
               ('broken_package_percent', lib.percent(total_brkn_dataset, num_packages)),
               ('num_broken_resources', total_brkn_link),
               ('num_resources', num_res),
               ('broken_resource_percent', lib.percent(total_brkn_link, num_res)),
               ('order', order),
               ('table', broken_links),
               )) )       

    return resultData

def tagless_report_option_combinations():
    for organization in lib.all_organizations(include_none=True):
            yield {'organization': organization,
                   'include_sub_organizations': False
                  }

def broken_report_option_combinations():
   sql = model.Session.query(model.Group.name.distinct().label('organization')) \
                .join(model.Package, model.Group.id == model.Package.owner_org) \
                .join(model.ResourceGroup, model.ResourceGroup.package_id == model.Package.id) \
                .join(model.Resource, model.Resource.resource_group_id == model.ResourceGroup.id) \
                .join(model.TaskStatus, model.TaskStatus.entity_id == model.Resource.id) \
                .filter(model.Group.is_organization == True) \
                .filter(between(cast(model.TaskStatus.value, Numeric(3)), 400, 600)) \
                .filter(cast(model.TaskStatus.value, Numeric(3)) != 511) \
                .filter(model.TaskStatus.key == 'error_code' )\
                .filter(model.Package.state == 'active') \
                .filter(model.Resource.state == 'active') \
                .filter(model.ResourceGroup.state == 'active') \
                .filter(model.Group.state == 'active') \
                .filter(model.Group.name == 'test-org-3')

   yield {'organization': None,
             'include_sub_organizations': False
            }
            
   for row in sql:
      yield {'organization': row.organization,
             'include_sub_organizations': False
            }

tagless_report_info = {
    'name': 'tagless-datasets',
    'description': 'Datasets which have no tags.',
    'option_defaults': OrderedDict((('organization', None),
                                    ('include_sub_organizations', False),
                                    )),
    'option_combinations': tagless_report_option_combinations,
    'generate': tagless_report,
    'template': 'report/tagless-datasets.html',
    }

broken_link_info = {

    'name': 'broken-links',
    'description': 'Dataset resource URLs that are found to result in errors when resolved.',
    'option_defaults': OrderedDict((('organization', None),
                                        ('include_sub_organizations', False),
                                        )),
    'option_combinations': broken_report_option_combinations,
    'generate': broken_link_report,
    'template': 'report/broken-links.html',

	}         