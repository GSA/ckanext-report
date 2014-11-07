import ckan.plugins as p
import ckan.lib.helpers
from pylons import request
from ckan import model
from sqlalchemy import or_
from ckan.lib.helpers import OrderedDict
import ckan.plugins.toolkit as t
c = t.c

def relative_url_for(**kwargs):
    '''Returns the existing URL but amended for the given url_for-style
    parameters. Much easier than calling h.add_url_param etc.
    '''
    args = dict(p.toolkit.request.environ['pylons.routes_dict'].items()
                + p.toolkit.request.params.items()
                + kwargs.items())
    # remove blanks
    for k, v in args.items():
        if not v:
            del args[k]
    return p.toolkit.url_for(**args)

def chunks(list_, size):
    '''Splits up a given list into 'size' sized chunks.'''
    for i in xrange(0, len(list_), size):
        yield list_[i:i+size]

def dgu_relative_url_for(**kwargs):
    '''Return the existing URL but amended for the given url_for-style
    parameters'''
    from ckan.lib.base import h
    args = dict(request.environ['pylons.routes_dict'].items()
                + request.params.items()
                + kwargs.items())
    # remove blanks
    for k, v in args.items():
        if not v:
            del args[k]
    return h.url_for(**args)


def user_link_info(user_name, organisation=None):  # Overwrite h.linked_user
    '''Given a user, return the display name and link to their profile page.
    '''
    from ckan.lib.base import h

    # Work out who the user is that we want to view
    user_name, user, user_drupal_id, type_, this_is_me = user_properties(user_name)

    # Now decide how to display the user
    if c.is_an_official is '':
        c.is_an_official = bool(c.groups or is_sysadmin())
    if (c.is_an_official or this_is_me or type_ is None):
        # User can see the actual user name - i.e. if:
        # * viewer is an official
        # * viewing ones own user
        # * viewing a member of public
        if user:
            if type_ == 'system':
                name = 'System Process (%s)' % user_name
            else:
                name = user.fullname or user.name
            if type_ == 'official' or not user_drupal_id:
                # officials use the CKAN user page for the time being (useful for debug)
                link_url = h.url_for(controller='user', action='read', id=user.name)
            else:
                # Public use the Drupal user page.
                link_url = '/users/%s' % user_drupal_id
            return (name, link_url)
        else:
            if type_ == 'system':
                user_name = 'System Process (%s)' % user_name
            return (user_name, None)
    else:
        # Can't see the user name - it gets anonymised.
        # Joe public just gets a link to the user's publisher(s)
        if user:
            groups = user.get_groups('organization')
            if type_ == 'official' and is_sysadmin(user):
                return ('System Administrator', None)
            elif groups:
                # We don't want to show all of the groups that the user belongs to.
                # We will try and match the organisation name if provided and use that
                # instead.  If none is provided, or we can't match one then we will use
                # the highest level org.
                matched_group = None
                for group in groups:
                    if group.title == organisation:
                        matched_group = group
                        break
                if not matched_group:
                    matched_group = groups[0]

                return (matched_group.title,
                       '/publisher/%s' % matched_group.name)
            elif type_ == 'system':
                return ('System Process', None)
            else:
                return ('Staff', None)
        else:
            return ('System process' if type_ == 'system' else 'Staff', None)


def organization_list():
    organizations = model.Session.query(model.Group).\
        filter(model.Group.type=='organization').\
        filter(model.Group.state=='active').order_by('title')
    for organization in organizations:
        yield (organization.name, organization.title)

def is_sysadmin(u=None):
    from ckan import new_authz, model
    user = u or c.userobj
    if not user:
        return False
    if isinstance(user, model.User):
        return user.sysadmin
    return new_authz.is_sysadmin(user)

def render_datetime(datetime_, date_format=None, with_hours=False):
    '''Render a datetime object or timestamp string as a pretty string
    (Y-m-d H:m).
    If timestamp is badly formatted, then a blank string is returned.

    This is a wrapper on the CKAN one which has an American date_format.
    '''
    if not date_format:
        date_format = '%d %b %Y'
        if with_hours:
            date_format += ' %H:%M'
    return ckan.lib.helpers.render_datetime(datetime_, date_format)

