'''
Working examples - simple tag report.
'''

from ckan import model
from ckan.lib.helpers import OrderedDict
from ckanext.report import lib
from sqlalchemy import func
from sqlalchemy import or_

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
    if organization:
        q = lib.filter_by_organizations(q, organization,
                                        include_sub_organizations)
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

    q = model.Session.query(model.Group.name.label('grp_name'), model.Group.title.label('grp_title'),  \
              func.count(model.Package.name.distinct()).label('dataset_cnt'), \
              func.count(model.Group.name).label('broken_link_cnt')) \
             .join(model.Package, model.Group.id == model.Package.owner_org) \
             .join(model.ResourceGroup, model.ResourceGroup.package_id == model.Package.id) \
             .join(model.Resource, model.Resource.resource_group_id == model.ResourceGroup.id) \
             .join(model.TaskStatus, model.TaskStatus.entity_id == model.Resource.id) \
             .filter(model.Group.is_organization == True) \
             .filter(or_(model.TaskStatus.value == 'URL unobtainable: Server returned HTTP 404',\
                         model.TaskStatus.value == 'Connection timed out after 30s', \
                         model.TaskStatus.value == 'Invalid URL'\
                         model.TaskStatus.value == 'URL unobtainable: Server returned HTTP 400'\
                         model.TaskStatus.value == 'Server returned error: Internal server error on the remote server'\
                         model.TaskStatus.value == 'URL unobtainable: Server returned HTTP 403'\
                         model.TaskStatus.value == 'Server returned error: Service unavailable'\
                         model.TaskStatus.value == 'Server returned error: 405 Method Not Allowed',\
                         model.TaskStatus.value == 'Could not make HEAD request')) \
             .filter(model.Package.state == 'active') \
             .filter(model.Resource.state == 'active') \
             .filter(model.ResourceGroup.state == 'active') \
             .filter(model.Group.state == 'active') \
             .group_by(model.Group.name, model.Group.title) \
             .order_by(model.Group.title)
    

    if organization:
        q = lib.filter_by_organizations(q, organization,
                                        include_sub_organizations)

    broken_links = []

    for row in q:

      broken_links.append(OrderedDict((
               ('grp_name', row.grp_name),
               ('grp_title', row.grp_title),
               ('dataset_count', row.dataset_cnt),
               ('broken_link_count', row.broken_link_cnt),
               ('per_broken_link', lib.percent(row.broken_link_cnt, grp_totals[row.grp_name]['total_grp_res_cnt'])),
            )) )
            
    q = q.subquery()
    q1 = model.Session.query(func.sum(q.c.dataset_cnt).label('total_brkn_dataset'), func.sum(q.c.broken_link_cnt).label('total_brkn_link')).first()

    total_brkn_dataset = q1.total_brkn_dataset	     
    total_brkn_link = q1.total_brkn_link

    # Number of resources and package
    q = model.Session.query(model.Package)\
                           .filter(model.Package.state == 'active')

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
               ('table', broken_links),
               ('num_packages', num_packages),
               ('num_res', num_res),
               ('total_brkn_dataset', total_brkn_dataset),
               ('total_brkn_link', total_brkn_link),
               )) )

    return resultData

def report_option_combinations():
    for organization in lib.all_organizations(include_none=True):
        for include_sub_organizations in (False, True):
            yield {'organization': organization,
                   'include_sub_organizations': include_sub_organizations}

tagless_report_info = {
    'name': 'tagless-datasets',
    'description': 'Datasets which have no tags.',
    'option_defaults': OrderedDict((('organization', None),
                                    ('include_sub_organizations', False),
                                    )),
    'option_combinations': report_option_combinations,
    'generate': tagless_report,
    'template': 'report/tagless-datasets.html',
    }

broken_link_info = {

    'name': 'broken-links',
    'description': 'Dataset resource URLs that are found to result in errors when resolved.',
    'option_defaults': OrderedDict((('organization', None),
                                        ('include_sub_organizations', False),
                                        )),
    'option_combinations': report_option_combinations,
    'generate': broken_link_report,
    'template': 'report/broken-links.html',

	}         
